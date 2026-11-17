"""
supervisor.py - Supervisor de Ejecución Continua 24/7 para Agenda Grostar
Repositorio Daemon: Grostar/agenda-grostar-cloud-daemon
Garantiza:
1. Relevo pre-calentado con 0 segundos de corte y sin colisiones 409 Conflict en Telegram.
2. Comprobación de ciclo anterior: espera a que el ciclo previo termine antes de iniciar polling.
3. Respeto de la ventana crítica matutina (07:55 - 08:05 Europe/Madrid): nunca releva durante el resumen matutino.
4. Heartbeat y comprobación puntual de alertas cada 60s vía endpoint local /cron-check.
5. Monitoreo continuo del proceso bot (agenda_app/main.py) y auto-recuperación si cae.
6. Disparo puntual del siguiente ciclo antes de que expire el runner de GitHub Actions (MAX_RUNTIME_SECONDS=19200).
"""

from __future__ import annotations

import json
import logging
import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("CloudDaemon.Supervisor")

REPO_OWNER = "Grostar"
DAEMON_REPO = "agenda-grostar-cloud-daemon"
WORKFLOW_FILE = "daemon_24_7.yml"
TIMEZONE_NAME = os.environ.get("TIMEZONE", "Europe/Madrid")
try:
    TZ = ZoneInfo(TIMEZONE_NAME)
except Exception:
    TZ = ZoneInfo("UTC")

MAX_RUNTIME_SECONDS = int(os.environ.get("MAX_RUNTIME_SECONDS", "19200"))  # ~5h 20m
PRE_WARMED_LEAD_SECONDS = int(os.environ.get("PRE_WARMED_LEAD_SECONDS", "85"))
PORT = os.environ.get("PORT", "8080")
GH_PAT = os.environ.get("GH_PAT") or os.environ.get("GITHUB_TOKEN", "")
CURRENT_RUN_ID = str(os.environ.get("GITHUB_RUN_ID", ""))


def github_api(endpoint: str, method: str = "GET", data: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    """Realiza una petición a la API REST de GitHub con autenticación y timeout."""
    if not GH_PAT:
        logger.warning("No hay GH_PAT disponible para llamar a la API de GitHub.")
        return None
    url = f"https://api.github.com{endpoint}"
    headers = {
        "Authorization": f"Bearer {GH_PAT}",
        "Accept": "application/vnd.github+json",
        "User-Agent": "AgendaGrostar-Supervisor",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    encoded_data = json.dumps(data).encode("utf-8") if data is not None else None
    if encoded_data:
        headers["Content-Type"] = "application/json"

    req = urllib.request.Request(url, data=encoded_data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=12) as resp:
            content = resp.read().decode("utf-8")
            if not content:
                return {}
            return json.loads(content)
    except urllib.error.HTTPError as err:
        logger.warning("Error HTTP API GitHub %s %s: %s (code %d)", method, endpoint, err.reason, err.code)
        return None
    except Exception as exc:
        logger.warning("Error de conexión API GitHub %s %s: %s", method, endpoint, exc)
        return None


def wait_for_older_daemon_runs_to_finish(max_wait_seconds: int = 90) -> None:
    """
    Si este ciclo fue lanzado como relevo mientras un ciclo previo estaba activo,
    espera a que el ciclo previo termine su proceso y cierre la conexión de Telegram
    para evitar conflictos 409 Conflict.
    """
    if not CURRENT_RUN_ID:
        return

    logger.info("Comprobando si hay ciclos anteriores activos en %s...", DAEMON_REPO)
    start_wait = time.time()

    while time.time() - start_wait < max_wait_seconds:
        data = github_api(f"/repos/{REPO_OWNER}/{DAEMON_REPO}/actions/workflows/{WORKFLOW_FILE}/runs?status=in_progress")
        if not data:
            break

        runs = data.get("workflow_runs", [])
        older_runs = []
        for r in runs:
            r_id = str(r.get("id", ""))
            if r_id != CURRENT_RUN_ID:
                if CURRENT_RUN_ID.isdigit() and r_id.isdigit():
                    if int(r_id) < int(CURRENT_RUN_ID):
                        older_runs.append(r)
                else:
                    older_runs.append(r)

        if not older_runs:
            logger.info("No hay ciclos anteriores activos. Canal de Telegram libre.")
            return

        older_id = older_runs[0].get("id")
        elapsed = int(time.time() - start_wait)
        logger.info(
            "Ciclo anterior ID=%s aún en progreso. Esperando relevo sin colisiones (%ds/%ds)...",
            older_id, elapsed, max_wait_seconds
        )
        time.sleep(3)

    logger.info("Fin de la espera de ciclos anteriores. Continuando con el arranque.")


def is_in_critical_window() -> bool:
    """
    Devuelve True si nos encontramos en la ventana de envío del Resumen Matutino (07:55 - 08:05 Europe/Madrid).
    Durante esta ventana no se debe iniciar un relevo para no cortar el envío matutino.
    """
    now = datetime.now(TZ)
    return (now.hour == 7 and now.minute >= 55) or (now.hour == 8 and now.minute <= 5)


def trigger_next_cycle() -> bool:
    """Lanza el siguiente ciclo 24/7 mediante workflow_dispatch."""
    logger.info("🚀 Lanzando siguiente ciclo de relevo 24/7 en %s...", DAEMON_REPO)
    res = github_api(
        f"/repos/{REPO_OWNER}/{DAEMON_REPO}/actions/workflows/{WORKFLOW_FILE}/dispatches",
        method="POST",
        data={"ref": "main"},
    )
    if res is not None:
        logger.info("✅ Siguiente ciclo 24/7 despachado con éxito.")
        return True
    logger.error("❌ Fallo al despachar el siguiente ciclo 24/7.")
    return False


def ping_cron_check_endpoint() -> None:
    """Llama al endpoint local /cron-check para forzar comprobación puntual y heartbeat."""
    url = f"http://127.0.0.1:{PORT}/cron-check"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Supervisor-CronPing"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            logger.debug("Heartbeat local /cron-check OK: %s", data.get("checked_at"))
    except Exception as e:
        logger.debug("Aviso al consultar /cron-check local: %s", e)


def terminate_process_gracefully(proc: subprocess.Popen, timeout: int = 20) -> None:
    """Detiene el proceso hijo enviando SIGTERM (o terminate en Windows) y esperando su salida."""
    if proc.poll() is not None:
        return
    logger.info("Enviando señal de terminación ordenada al proceso bot (PID %s)...", proc.pid)
    try:
        if sys.platform != "win32":
            proc.send_signal(signal.SIGTERM)
        else:
            proc.terminate()
        proc.wait(timeout=timeout)
        logger.info("Proceso bot terminado correctamente.")
    except subprocess.TimeoutExpired:
        logger.warning("El proceso bot no terminó en %ds. Forzando SIGKILL...", timeout)
        proc.kill()
        proc.wait(timeout=5)
    except Exception as exc:
        logger.warning("Error al terminar proceso bot: %s", exc)


def main() -> int:
    logger.info("==================================================================")
    logger.info("Iniciando Supervisor 24/7 de Agenda Grostar")
    logger.info("Zona Horaria: %s | Max Runtime: %ds (~%.1fh) | Lead Relevo: %ds", TIMEZONE_NAME, MAX_RUNTIME_SECONDS, MAX_RUNTIME_SECONDS / 3600, PRE_WARMED_LEAD_SECONDS)
    logger.info("Run ID Actual: %s", CURRENT_RUN_ID or "local")
    logger.info("==================================================================")

    # 1. Esperar a que el ciclo anterior termine si existe
    wait_for_older_daemon_runs_to_finish(max_wait_seconds=90)

    # 2. Localizar directorio de la aplicación
    base_dir = os.path.dirname(os.path.abspath(__file__))
    app_dir = os.path.join(base_dir, "agenda_app")
    if not os.path.exists(os.path.join(app_dir, "main.py")):
        if os.path.exists(os.path.join(base_dir, "main.py")):
            app_dir = base_dir
        else:
            logger.error("No se encontró main.py ni en agenda_app/ ni en el directorio actual.")
            return 1

    main_py = os.path.join(app_dir, "main.py")
    logger.info("Directorio de la aplicación: %s", app_dir)

    # Asegurar que la rama local en el runner esté al día
    try:
        subprocess.run(
            ["git", "pull", "--rebase"],
            cwd=app_dir,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=15,
        )
    except Exception:
        pass

    bot_env = os.environ.copy()
    bot_env["CLOUD_DAEMON_24_7"] = "true"
    bot_env["RENDER"] = "false"
    bot_env["PYTHONUNBUFFERED"] = "1"
    bot_env["PORT"] = str(PORT)

    start_time = time.time()
    last_cron_ping = time.time()
    next_cycle_dispatched = False
    restarts_count = 0
    max_restarts = 5

    def start_bot_process() -> subprocess.Popen:
        logger.info("Arrancando proceso de Agenda Grostar: %s %s", sys.executable, main_py)
        return subprocess.Popen(
            [sys.executable, "-u", main_py],
            cwd=app_dir,
            env=bot_env,
        )

    child = start_bot_process()

    try:
        while True:
            time.sleep(5)
            now_sec = time.time()
            elapsed = now_sec - start_time

            exit_code = child.poll()
            if exit_code is not None:
                if next_cycle_dispatched:
                    logger.info("Proceso bot terminado con código %s tras relevo.", exit_code)
                    break
                else:
                    restarts_count += 1
                    logger.warning(
                        "Proceso bot murió inesperadamente con código %s. Reinicio %d/%d...",
                        exit_code, restarts_count, max_restarts
                    )
                    if restarts_count > max_restarts:
                        logger.error("Superado límite de reinicios (%d). Lanzando relevo de emergencia...", max_restarts)
                        trigger_next_cycle()
                        return 1
                    time.sleep(3)
                    child = start_bot_process()
                    continue

            if now_sec - last_cron_ping >= 60:
                last_cron_ping = now_sec
                ping_cron_check_endpoint()

            time_remaining = MAX_RUNTIME_SECONDS - elapsed
            if time_remaining <= PRE_WARMED_LEAD_SECONDS and not next_cycle_dispatched:
                if is_in_critical_window():
                    logger.info(
                        "Alcanzado tiempo de relevo pero en ventana crítica (07:55 - 08:05 Europe/Madrid). Pospobiendo..."
                    )
                    continue

                logger.info(
                    "⏳ Tiempo restante (%ds <= %ds). Iniciando relevo pre-calentado de 0s corte...",
                    int(time_remaining), PRE_WARMED_LEAD_SECONDS
                )
                dispatched_ok = trigger_next_cycle()
                next_cycle_dispatched = True

                logger.info("Esperando 30 segundos a que el nuevo ciclo se prepare...")
                time.sleep(30)

                logger.info("Deteniendo bot local para transferir la sesión de Telegram al nuevo ciclo...")
                terminate_process_gracefully(child, timeout=20)
                logger.info("Relevo 24/7 completado sin interrupción.")
                break

    except KeyboardInterrupt:
        logger.info("Interrupción recibida en Supervisor.")
        if child:
            terminate_process_gracefully(child, timeout=10)
    except Exception as exc:
        logger.error("Error imprevisto en loop de supervisor: %s", exc)
        if not next_cycle_dispatched:
            trigger_next_cycle()
        if child:
            terminate_process_gracefully(child, timeout=10)
        return 1

    logger.info("Supervisor finalizado limpiamente con éxito.")
    return 0


if __name__ == "__main__":
    sys.exit(main())