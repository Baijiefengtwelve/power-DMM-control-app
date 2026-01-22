from __future__ import annotations

import os
import signal
import subprocess
import sys
import threading
import time
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List, Dict, Any

# Windows: hide console window for child processes
CREATE_NO_WINDOW = 0x08000000 if os.name == "nt" else 0


def _append_log(path: Path, msg: str) -> None:
    """
    Best-effort log helper. In PyInstaller -w mode there is no console,
    so we log to files under the runtime root (exe directory).
    """
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write(msg.rstrip() + "\n")
    except Exception:
        pass


def build_uvicorn_log_config(log_file: Optional[Path] = None) -> Dict[str, Any]:
    """
    Uvicorn default logging tries to auto-detect colors via stream.isatty().
    In PyInstaller -w mode, sys.stdout/sys.stderr can be None which breaks that.
    We force use_colors=False and optionally log to a file.
    """
    if log_file:
        handlers = {
            "default": {
                "class": "logging.FileHandler",
                "formatter": "default",
                "filename": str(log_file),
                "encoding": "utf-8",
            },
            "access": {
                "class": "logging.FileHandler",
                "formatter": "access",
                "filename": str(log_file),
                "encoding": "utf-8",
            },
        }
    else:
        handlers = {
            "default": {
                "class": "logging.StreamHandler",
                "formatter": "default",
                "stream": "ext://sys.stderr",
            },
            "access": {
                "class": "logging.StreamHandler",
                "formatter": "access",
                "stream": "ext://sys.stdout",
            },
        }

    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "()": "uvicorn.logging.DefaultFormatter",
                "fmt": "%(levelprefix)s %(message)s",
                "use_colors": False,
            },
            "access": {
                "()": "uvicorn.logging.AccessFormatter",
                "fmt": '%(levelprefix)s %(client_addr)s - "%(request_line)s" %(status_code)s',
                "use_colors": False,
            },
        },
        "handlers": handlers,
        "loggers": {
            "uvicorn": {"handlers": ["default"], "level": "INFO", "propagate": False},
            "uvicorn.error": {"handlers": ["default"], "level": "INFO", "propagate": False},
            "uvicorn.access": {"handlers": ["access"], "level": "INFO", "propagate": False},
        },
    }


@dataclass
class StartedProcess:
    name: str
    popen: subprocess.Popen
    mode: str  # "influxd" | "docker"
    cmd: List[str]
    workdir: Optional[str] = None


class InfluxDBManager:
    """
    Starts InfluxDB in one of two modes:
      1) Embedded binary mode: tools/influxdb/influxd(.exe)
      2) Docker Compose mode: monitoring/docker-compose.yml (requires Docker Desktop)

    Stops it when application exits.
    """

    def __init__(self, project_root: Path, log_path: Optional[Path] = None):
        self.project_root = Path(project_root).resolve()
        self._proc: Optional[StartedProcess] = None
        self.log_path = log_path or (self.project_root / "influx.log")

    def _popen(self, cmd: List[str], cwd: Optional[Path] = None) -> subprocess.Popen:
        kwargs = dict(
            cwd=str(cwd) if cwd else None,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        if os.name == "nt":
            kwargs["creationflags"] = CREATE_NO_WINDOW
        return subprocess.Popen(cmd, **kwargs)

    def start(self) -> None:
        if self._proc is not None:
            return

        # Prefer embedded InfluxDB binary if present
        influx_bin = self.project_root / "tools" / "influxdb" / ("influxd.exe" if os.name == "nt" else "influxd")
        if influx_bin.exists():
            data_dir = self.project_root / "monitoring" / "influxdb-data"
            data_dir.mkdir(parents=True, exist_ok=True)
            cmd = [
                str(influx_bin),
                "--engine-path", str(data_dir / "engine"),
                "--bolt-path", str(data_dir / "influxd.bolt"),
            ]
            _append_log(self.log_path, f"[Influx] starting embedded: {' '.join(cmd)}")
            try:
                p = self._popen(cmd, cwd=influx_bin.parent)
                self._proc = StartedProcess(name="InfluxDB", popen=p, mode="influxd", cmd=cmd, workdir=str(influx_bin.parent))
            except Exception as e:
                _append_log(self.log_path, f"[Influx] embedded start failed: {e}\n{traceback.format_exc()}")
            return

        # Fallback: docker compose
        compose_file = self.project_root / "monitoring" / "docker-compose.yml"
        if compose_file.exists():
            env_file = self.project_root / "monitoring" / ".env"
            example = self.project_root / "monitoring" / ".env.example"
            if (not env_file.exists()) and example.exists():
                try:
                    env_file.write_text(example.read_text(encoding="utf-8"), encoding="utf-8")
                    _append_log(self.log_path, f"[Influx] created default .env from .env.example")
                except Exception:
                    pass

            base_cmd = ["docker", "compose", "-f", str(compose_file)]
            if env_file.exists():
                base_cmd += ["--env-file", str(env_file)]
            up_cmd = base_cmd + ["up", "-d"]

            _append_log(self.log_path, f"[Influx] starting docker: {' '.join(up_cmd)}")

            try:
                p = self._popen(up_cmd, cwd=compose_file.parent)
            except FileNotFoundError:
                _append_log(self.log_path, "[Influx] docker not found; skip start")
                return
            except Exception as e:
                _append_log(self.log_path, f"[Influx] docker start failed: {e}\n{traceback.format_exc()}")
                return

            # Collect immediate output / exit code (docker compose up -d should exit quickly)
            out = ""
            try:
                out, _ = p.communicate(timeout=20)
            except Exception:
                pass
            rc = p.poll()
            if out:
                _append_log(self.log_path, f"[Influx] docker output:\n{out}")

            if rc not in (0, None):
                _append_log(self.log_path, f"[Influx] docker up failed rc={rc}; not marking as started")
                return

            # Mark started (mode docker). We keep a lightweight marker process handle.
            self._proc = StartedProcess(name="InfluxDB(docker)", popen=p, mode="docker", cmd=up_cmd, workdir=str(compose_file.parent))
            return

        _append_log(self.log_path, "[Influx] compose file not found; nothing to start")
        return

    def stop(self) -> None:
        if self._proc is None:
            return

        _append_log(self.log_path, f"[Influx] stopping mode={self._proc.mode}")
        try:
            if self._proc.mode == "docker":
                compose_file = self.project_root / "monitoring" / "docker-compose.yml"
                env_file = self.project_root / "monitoring" / ".env"
                base_cmd = ["docker", "compose", "-f", str(compose_file)]
                if env_file.exists():
                    base_cmd += ["--env-file", str(env_file)]
                down_cmd = base_cmd + ["down"]
                _append_log(self.log_path, f"[Influx] docker down: {' '.join(down_cmd)}")
                try:
                    p = self._popen(down_cmd, cwd=compose_file.parent)
                    try:
                        out, _ = p.communicate(timeout=30)
                        if out:
                            _append_log(self.log_path, f"[Influx] docker down output:\n{out}")
                    except Exception:
                        pass
                except FileNotFoundError:
                    _append_log(self.log_path, "[Influx] docker not found while stopping; skip")
                except Exception as e:
                    _append_log(self.log_path, f"[Influx] docker down failed: {e}\n{traceback.format_exc()}")

            else:
                p = self._proc.popen
                if p.poll() is None:
                    if os.name == "nt":
                        p.terminate()
                    else:
                        p.send_signal(signal.SIGTERM)
                try:
                    out, _ = p.communicate(timeout=10)
                    if out:
                        _append_log(self.log_path, f"[Influx] embedded output:\n{out}")
                except Exception:
                    pass
        finally:
            self._proc = None


class UvicornServerThread:
    """
    Runs uvicorn Server in a thread so we can signal it to exit cleanly.
    """

    def __init__(self, app, host: str = "127.0.0.1", port: int = 8000, log_level: str = "info", log_path: Optional[Path] = None):
        self.app = app
        self.host = host
        self.port = port
        self.log_level = log_level
        self.log_path = log_path  # if set, log to this file
        self._server = None
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return

        import uvicorn

        log_config = build_uvicorn_log_config(self.log_path)
        config = uvicorn.Config(
            self.app,
            host=self.host,
            port=self.port,
            log_level=self.log_level,
            access_log=False,
            log_config=log_config,
        )
        self._server = uvicorn.Server(config=config)

        def _run():
            self._server.run()

        self._thread = threading.Thread(target=_run, daemon=True)
        self._thread.start()

    def stop(self, timeout_s: float = 5.0) -> None:
        if not self._server:
            return
        try:
            self._server.should_exit = True
        except Exception:
            pass
        if self._thread:
            self._thread.join(timeout=timeout_s)
