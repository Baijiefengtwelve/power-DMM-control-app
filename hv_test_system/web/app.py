from __future__ import annotations

import asyncio
import json
import os
import uuid
from contextlib import asynccontextmanager, suppress
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
        if not clients:
            return
        results = await asyncio.gather(
            *(ws.send_text(data) for ws in clients),
            return_exceptions=True,
        )
        dead = [
            ws
            for ws, result in zip(clients, results)
            if isinstance(result, Exception)
        ]
        if dead:
            async with self._lock:
                for ws in dead:
                    self._clients.discard(ws)


class _WebAppBuilder:
    """Build and register the FastAPI web surface around a Qt main window."""

    def __init__(self, main_window, *, static_dir: Optional[str] = None):
        self.main_window = main_window
        self.app = FastAPI(
            title="HV Test System Web",
            version="1.0",
            lifespan=self._lifespan,
        )
        self.bridge = WebBridge(main_window)
        self.hub = _WSHub()
        self.static_path = self._resolve_static_path(static_dir)

    def create(self) -> FastAPI:
        self._configure_static_assets()
        self._register_state_routes()
        self._register_device_routes()
        self._register_test_routes()
        self._register_record_routes()
        self._register_file_routes()
        self._register_websocket_routes()
        self._register_index_route()
        return self.app

    def _resolve_static_path(self, static_dir: Optional[str]) -> Path:
        base_dir = Path(__file__).resolve().parent
        static_path = Path(static_dir) if static_dir else (base_dir / "static")
        static_path.mkdir(parents=True, exist_ok=True)
        return static_path

    def _configure_static_assets(self):
        self.app.mount("/static", StaticFiles(directory=str(self.static_path)), name="static")

    async def _run_cmd(self, action: str, params: Optional[Dict[str, Any]] = None, timeout_s: float = 8.0):
        import concurrent.futures

        cmd_id = str(uuid.uuid4())
        future: "concurrent.futures.Future" = concurrent.futures.Future()
        self.bridge.register_future(cmd_id, future)

        payload = {"id": cmd_id, "action": action, "params": params or {}}
        self.bridge.command_signal.emit(payload)

        try:
            return await asyncio.wait_for(asyncio.wrap_future(future), timeout=timeout_s)
        except asyncio.TimeoutError:
            return {"ok": False, "message": "timeout", "data": None}

    def _register_state_routes(self):
        @self.app.get("/api/state")
        async def api_state():
            return await self._run_cmd("get_state")

        @self.app.get("/api/influx_status")
        async def api_influx_status():
            try:
                influx_writer = getattr(self.main_window, "influx_writer", None)
                if influx_writer is None:
                    return {"ok": False, "message": "influx_writer not initialized", "data": None}
                return {"ok": True, "message": "OK", "data": influx_writer.status()}
            except Exception as exc:
                return {"ok": False, "message": str(exc), "data": None}

        @self.app.get("/api/db/stats")
        async def api_db_stats():
            return await self._run_cmd("db_stats")

        @self.app.post("/api/db/cleanup")
        async def api_db_cleanup(body: Dict[str, Any]):
            return await self._run_cmd("db_cleanup", body, timeout_s=30.0)

        @self.app.get("/api/plot")
        async def api_plot():
            return await self._run_cmd("get_plot")

    def _register_device_routes(self):
        @self.app.get("/api/gpib_ports")
        async def api_gpib_ports():
            return self._list_gpib_ports()

        @self.app.get("/api/ports")
        async def api_ports():
            return await self._run_cmd("list_ports")

        @self.app.post("/api/refresh_ports")
        async def api_refresh_ports():
            return await self._run_cmd("refresh_ports")

        @self.app.post("/api/hv/connect")
        async def api_hv_connect(body: Dict[str, Any]):
            return await self._run_cmd("hv_connect", body)

        @self.app.post("/api/hv/disconnect")
        async def api_hv_disconnect():
            return await self._run_cmd("hv_disconnect")

        @self.app.post("/api/keithley/connect")
        async def api_keithley_connect(body: Dict[str, Any]):
            return await self._run_cmd("keithley_connect", body)

        @self.app.post("/api/keithley/disconnect")
        async def api_keithley_disconnect():
            return await self._run_cmd("keithley_disconnect")

        @self.app.post("/api/meter/toggle")
        async def api_meter_toggle(body: Dict[str, Any]):
            return await self._run_cmd("meter_toggle", body)

        @self.app.post("/api/meter/coeff")
        async def api_meter_coeff(body: Dict[str, Any]):
            return await self._run_cmd("set_meter_coeff", body)

        @self.app.post("/api/params/test")
        async def api_set_test_params(body: Dict[str, Any]):
            return await self._run_cmd("set_test_params", body)

        @self.app.post("/api/params/stabilization")
        async def api_set_stab_params(body: Dict[str, Any]):
            return await self._run_cmd("set_stabilization_params", body)

    def _register_test_routes(self):
        @self.app.post("/api/test/start")
        async def api_test_start():
            return await self._run_cmd("start_test")

        @self.app.post("/api/test/start_cycle")
        async def api_test_start_cycle():
            return await self._run_cmd("start_cycle_test")

        @self.app.post("/api/test/stop")
        async def api_test_stop():
            return await self._run_cmd("stop_test")

        @self.app.post("/api/emergency_stop")
        async def api_emergency_stop():
            return await self._run_cmd("emergency_stop")

        @self.app.post("/api/test/reset_voltage")
        async def api_test_reset_voltage():
            return await self._run_cmd("reset_voltage")

        @self.app.post("/api/stabilization/start")
        async def api_stab_start():
            return await self._run_cmd("start_stabilization")

        @self.app.post("/api/stabilization/stop")
        async def api_stab_stop():
            return await self._run_cmd("stop_stabilization")

        @self.app.post("/api/chart/clear")
        async def api_chart_clear():
            return await self._run_cmd("clear_chart")

    def _register_record_routes(self):
        @self.app.post("/api/record/path")
        async def api_record_path(body: Dict[str, Any]):
            return await self._run_cmd("set_record_path", body)

        @self.app.post("/api/record/toggle")
        async def api_record_toggle():
            return await self._run_cmd("toggle_recording")

        @self.app.get("/api/files")
        async def api_files():
            return self._list_record_files()

        @self.app.get("/download/{filename}")
        async def download_file(filename: str):
            file_path = self._resolve_download_file(filename)
            if not os.path.isfile(file_path):
                return JSONResponse({"ok": False, "message": "file not found"}, status_code=404)
            return FileResponse(file_path, filename=os.path.basename(file_path))

    def _register_file_routes(self):
        return None

    def _register_websocket_routes(self):
        @self.app.websocket("/ws/telemetry")
        async def ws_telemetry(ws: WebSocket):
            await self.hub.add(ws)
            try:
                while True:
                    await asyncio.sleep(10)
            except WebSocketDisconnect:
                await self.hub.remove(ws)
            except Exception:
                await self.hub.remove(ws)

    def _register_index_route(self):
        @self.app.get("/")
        async def index():
            index_file = self.static_path / "index.html"
            if index_file.exists():
                return HTMLResponse(index_file.read_text(encoding="utf-8"))
            return HTMLResponse("<h3>Web UI not found</h3>")

    def _list_gpib_ports(self):
        try:
            try:
                import pyvisa  # type: ignore

                resource_manager = pyvisa.ResourceManager()
                resources = resource_manager.list_resources()
                gpib_resources = [name for name in resources if ("GPIB" in name) or ("gpib" in name.lower())]
                if gpib_resources:
                    return {"ok": True, "message": "OK", "data": gpib_resources}
                return {
                    "ok": True,
                    "message": "No GPIB resources found",
                    "data": [str(i) for i in range(0, 31)],
                }
            except Exception:
                return {
                    "ok": True,
                    "message": "VISA unavailable, fallback addresses",
                    "data": [str(i) for i in range(0, 31)],
                }
        except Exception as exc:
            return {"ok": False, "message": str(exc), "data": []}

    def _list_record_files(self):
        try:
            record_dir = self.main_window.get_record_output_dir()
        except Exception:
            record_dir = ""

        if not record_dir or not os.path.isdir(record_dir):
            return {"ok": True, "message": "no directory", "data": {"path": record_dir, "files": []}}

        files = []
        for filename in os.listdir(record_dir):
            if not filename.lower().endswith((".xlsx", ".csv")):
                continue
            full_path = os.path.join(record_dir, filename)
            try:
                stat = os.stat(full_path)
                files.append({"name": filename, "size": stat.st_size, "mtime": int(stat.st_mtime)})
            except Exception:
                files.append({"name": filename})
        files.sort(key=lambda item: item.get("mtime", 0), reverse=True)
        return {"ok": True, "message": "OK", "data": {"path": record_dir, "files": files}}

    def _resolve_download_file(self, filename: str) -> str:
        try:
            return self.main_window.resolve_record_download_path(filename)
        except Exception:
            return ""

    async def _telemetry_loop(self):
        while True:
            state = await self._safe_run_cmd("get_state", timeout_s=3.0, fallback={"ok": False, "message": "get_state failed", "data": {}})
            plot = await self._safe_run_cmd("get_plot", timeout_s=3.0, fallback={"ok": False, "message": "get_plot failed", "data": {"t": []}})

            try:
                await self.hub.broadcast_json({"type": "telemetry", "state": state, "plot": plot})
            except Exception:
                pass

            await asyncio.sleep(0.5)

    async def _safe_run_cmd(self, action: str, *, timeout_s: float, fallback):
        try:
            return await self._run_cmd(action, timeout_s=timeout_s)
        except Exception:
            return fallback

    @asynccontextmanager
    async def _lifespan(self, _app):
        telemetry_task = asyncio.create_task(self._telemetry_loop())
        try:
            yield
        finally:
            telemetry_task.cancel()
            with suppress(asyncio.CancelledError):
                await telemetry_task


def create_app(main_window, *, static_dir: Optional[str] = None) -> FastAPI:
    """Create a FastAPI application bound to an existing Qt MainWindow instance."""
    return _WebAppBuilder(main_window, static_dir=static_dir).create()
