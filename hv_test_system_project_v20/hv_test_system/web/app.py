from __future__ import annotations

import asyncio
import json
import os
import threading
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from .bridge import WebBridge


class _WSHub:
    def __init__(self):
        self._lock = asyncio.Lock()
        self._clients: set[WebSocket] = set()

    async def add(self, ws: WebSocket):
        await ws.accept()
        async with self._lock:
            self._clients.add(ws)

    async def remove(self, ws: WebSocket):
        async with self._lock:
            self._clients.discard(ws)

    async def broadcast_json(self, obj: Any):
        data = json.dumps(obj, ensure_ascii=False)
        async with self._lock:
            clients = list(self._clients)
        dead = []
        for ws in clients:
            try:
                await ws.send_text(data)
            except Exception:
                dead.append(ws)
        if dead:
            async with self._lock:
                for ws in dead:
                    self._clients.discard(ws)


def create_app(main_window, *, static_dir: Optional[str] = None) -> FastAPI:
    """
    Create a FastAPI application bound to an existing Qt MainWindow instance.

    The Qt event loop remains the "source of truth" for device I/O and threads.
    FastAPI runs in a background thread and sends commands to Qt via WebBridge.
    """
    app = FastAPI(title="HV Test System Web", version="1.0")
    bridge = WebBridge(main_window)

    # static files
    base_dir = Path(__file__).resolve().parent
    static_path = Path(static_dir) if static_dir else (base_dir / "static")
    static_path.mkdir(parents=True, exist_ok=True)

    app.mount("/static", StaticFiles(directory=str(static_path)), name="static")

    hub = _WSHub()

    async def _run_cmd(action: str, params: Optional[Dict[str, Any]] = None, timeout_s: float = 8.0):
        import concurrent.futures

        cmd_id = str(uuid.uuid4())
        fut: "concurrent.futures.Future" = concurrent.futures.Future()
        bridge.register_future(cmd_id, fut)

        payload = {"id": cmd_id, "action": action, "params": params or {}}
        # emit from this thread; Qt will queue it to main thread
        bridge.command_signal.emit(payload)

        try:
            result = await asyncio.wait_for(asyncio.wrap_future(fut), timeout=timeout_s)
            return result
        except asyncio.TimeoutError:
            return {"ok": False, "message": "timeout", "data": None}

    @app.get("/api/state")
    async def api_state():
        return await _run_cmd("get_state")

    @app.get("/api/influx_status")
    async def api_influx_status():
        """Expose InfluxDB writer diagnostics (useful when dashboards show no data)."""
        try:
            iw = getattr(main_window, "influx_writer", None)
            if iw is None:
                return {"ok": False, "message": "influx_writer not initialized", "data": None}
            return {"ok": True, "message": "OK", "data": iw.status()}
        except Exception as e:
            return {"ok": False, "message": str(e), "data": None}

    @app.get("/api/db/stats")
    async def api_db_stats():
        return await _run_cmd("db_stats")

    @app.post("/api/db/cleanup")
    async def api_db_cleanup(body: Dict[str, Any]):
        return await _run_cmd("db_cleanup", body, timeout_s=30.0)

    @app.get("/api/plot")
    async def api_plot():
        return await _run_cmd("get_plot")


    @app.get("/api/gpib_ports")
    async def api_gpib_ports():
        """
        List available GPIB VISA resources (preferred). If VISA is unavailable or returns none,
        return a fallback list of numeric addresses (0-30) so the UI can still provide a dropdown.
        """
        try:
            try:
                import pyvisa  # type: ignore
                rm = pyvisa.ResourceManager()
                resources = rm.list_resources()
                gpib = [r for r in resources if ("GPIB" in r) or ("gpib" in r.lower())]
                if gpib:
                    return {"ok": True, "message": "OK", "data": gpib}
                return {"ok": True, "message": "No GPIB resources found", "data": [str(i) for i in range(0, 31)]}
            except Exception:
                return {"ok": True, "message": "VISA unavailable, fallback addresses", "data": [str(i) for i in range(0, 31)]}
        except Exception as e:
            return {"ok": False, "message": f"{e}", "data": []}

    @app.get("/api/ports")
    async def api_ports():
        return await _run_cmd("list_ports")

    @app.post("/api/refresh_ports")
    async def api_refresh_ports():
        return await _run_cmd("refresh_ports")

    @app.post("/api/hv/connect")
    async def api_hv_connect(body: Dict[str, Any]):
        return await _run_cmd("hv_connect", body)

    @app.post("/api/hv/disconnect")
    async def api_hv_disconnect():
        return await _run_cmd("hv_disconnect")

    @app.post("/api/keithley/connect")
    async def api_keithley_connect(body: Dict[str, Any]):
        return await _run_cmd("keithley_connect", body)

    @app.post("/api/keithley/disconnect")
    async def api_keithley_disconnect():
        return await _run_cmd("keithley_disconnect")

    @app.post("/api/meter/toggle")
    async def api_meter_toggle(body: Dict[str, Any]):
        return await _run_cmd("meter_toggle", body)

    
    @app.post("/api/meter/coeff")
    async def api_meter_coeff(body: Dict[str, Any]):
        return await _run_cmd("set_meter_coeff", body)

    @app.post("/api/params/test")
    async def api_set_test_params(body: Dict[str, Any]):
        return await _run_cmd("set_test_params", body)

    @app.post("/api/params/stabilization")
    async def api_set_stab_params(body: Dict[str, Any]):
        return await _run_cmd("set_stabilization_params", body)

    @app.post("/api/test/start")
    async def api_test_start():
        return await _run_cmd("start_test")

    @app.post("/api/test/start_cycle")
    async def api_test_start_cycle():
        return await _run_cmd("start_cycle_test")

    @app.post("/api/test/stop")
    async def api_test_stop():
        return await _run_cmd("stop_test")

    @app.post("/api/test/reset_voltage")
    async def api_test_reset_voltage():
        return await _run_cmd("reset_voltage")

    @app.post("/api/stabilization/start")
    async def api_stab_start():
        return await _run_cmd("start_stabilization")

    @app.post("/api/stabilization/stop")
    async def api_stab_stop():
        return await _run_cmd("stop_stabilization")

    @app.post("/api/record/path")
    async def api_record_path(body: Dict[str, Any]):
        return await _run_cmd("set_record_path", body)

    @app.post("/api/record/toggle")
    async def api_record_toggle():
        return await _run_cmd("toggle_recording")

    @app.post("/api/chart/clear")
    async def api_chart_clear():
        return await _run_cmd("clear_chart")

    # Simple file listing/downloading (Excel output)
    @app.get("/api/files")
    async def api_files():
        # DataSaver likely saves to mw.save_path; list that dir
        try:
            path = getattr(main_window, "save_path", "") or ""
        except Exception:
            path = ""
        if not path or not os.path.isdir(path):
            return {"ok": True, "message": "no directory", "data": {"path": path, "files": []}}

        files = []
        for fn in os.listdir(path):
            if fn.lower().endswith((".xlsx", ".csv")):
                full = os.path.join(path, fn)
                try:
                    st = os.stat(full)
                    files.append({"name": fn, "size": st.st_size, "mtime": int(st.st_mtime)})
                except Exception:
                    files.append({"name": fn})
        files.sort(key=lambda x: x.get("mtime", 0), reverse=True)
        return {"ok": True, "message": "OK", "data": {"path": path, "files": files}}

    @app.get("/download/{filename}")
    async def download_file(filename: str):
        try:
            path = getattr(main_window, "save_path", "") or ""
        except Exception:
            path = ""
        file_path = os.path.join(path, filename)
        if not os.path.isfile(file_path):
            return JSONResponse({"ok": False, "message": "file not found"}, status_code=404)
        return FileResponse(file_path, filename=filename)

    @app.websocket("/ws/telemetry")
    async def ws_telemetry(ws: WebSocket):
        await hub.add(ws)
        try:
            while True:
                await asyncio.sleep(10)
        except WebSocketDisconnect:
            await hub.remove(ws)
        except Exception:
            await hub.remove(ws)

    async def telemetry_loop():
        while True:
            # Always try to broadcast state (for button/config sync), even if plot fails.
            try:
                state = await _run_cmd("get_state", timeout_s=3.0)
            except Exception:
                state = {"ok": False, "message": "get_state failed", "data": {}}

            try:
                plot = await _run_cmd("get_plot", timeout_s=3.0)
            except Exception:
                plot = {"ok": False, "message": "get_plot failed", "data": {"t": []}}

            try:
                await hub.broadcast_json({"type": "telemetry", "state": state, "plot": plot})
            except Exception:
                pass

            await asyncio.sleep(0.5)  # 2 Hz

    @app.on_event("startup")
    async def _on_startup():
        asyncio.create_task(telemetry_loop())

    # Index
    @app.get("/")
    async def index():
        index_file = static_path / "index.html"
        if index_file.exists():
            return HTMLResponse(index_file.read_text(encoding="utf-8"))
        return HTMLResponse("<h3>Web UI not found</h3>")

    return app
