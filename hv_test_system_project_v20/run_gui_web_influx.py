import argparse
import sys
import traceback
from pathlib import Path

from PyQt5.QtWidgets import (
    QApplication,
    QMessageBox,
    QSystemTrayIcon,
    QMenu,
    QAction,
)
from PyQt5.QtGui import QIcon
from PyQt5.QtCore import Qt

from hv_test_system import MainWindow
from hv_test_system.web import create_app
from hv_test_system.service_manager import UvicornServerThread, InfluxDBManager


def get_runtime_root() -> Path:
    # PyInstaller frozen: use exe directory
    if getattr(sys, "frozen", False) and hasattr(sys, "executable"):
        return Path(sys.executable).resolve().parent
    # dev: this file's directory
    return Path(__file__).resolve().parent


def append_log(path: Path, msg: str) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write(msg.rstrip() + "\n")
    except Exception:
        pass


def setup_tray(app: QApplication, window: MainWindow, root: Path) -> QSystemTrayIcon:
    """Create a system tray icon so closing the GUI hides it instead of exiting.

    - Left click / double click: show main window
    - Context menu: Show / Quit
    """
    app.setQuitOnLastWindowClosed(False)

    # Use a reasonable default icon (fallback to a simple bundled one if present)
    icon_path = root / "hv_test_system" / "web" / "static" / "favicon.ico"
    icon = QIcon(str(icon_path)) if icon_path.exists() else window.windowIcon()
    if icon.isNull():
        # Qt may return null icon; build a simple one from the style
        try:
            icon = app.style().standardIcon(app.style().SP_ComputerIcon)
        except Exception:
            icon = QIcon()

    tray = QSystemTrayIcon(icon, parent=window)
    tray.setToolTip("采集控制程序V20")

    menu = QMenu()
    act_show = QAction("显示主界面", menu)
    act_quit = QAction("退出", menu)
    menu.addAction(act_show)
    menu.addSeparator()
    menu.addAction(act_quit)
    tray.setContextMenu(menu)

    def _show_window():
        try:
            window.showNormal()
            window.raise_()
            window.activateWindow()
        except Exception:
            try:
                window.show()
            except Exception:
                pass

    def _quit_app():
        """Quit from tray: ensure full shutdown (Web/Influx) and remove tray icon."""
        try:
            window.request_quit()
        except Exception:
            pass
        # Remove tray icon immediately to avoid lingering icon if shutdown is slow.
        try:
            tray.hide()
            tray.deleteLater()
        except Exception:
            pass
        # Explicitly quit event loop (emits aboutToQuit for service shutdown).
        try:
            app.quit()
        except Exception:
            pass

    act_show.triggered.connect(_show_window)
    act_quit.triggered.connect(_quit_app)

    def _on_activated(reason):
        if reason in (QSystemTrayIcon.Trigger, QSystemTrayIcon.DoubleClick):
            _show_window()

    tray.activated.connect(_on_activated)
    tray.show()

    # Expose tray handle for optional notifications
    window.tray_icon = tray
    return tray


def main():
    parser = argparse.ArgumentParser(description="HV Test System (GUI + Web + InfluxDB)")
    parser.add_argument("--host", default="127.0.0.1", help="Web host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8000, help="Web port (default: 8000)")
    parser.add_argument("--no-web", action="store_true", help="Disable web service")
    parser.add_argument("--no-influx", action="store_true", help="Disable InfluxDB startup")
    parser.add_argument("--hidden", action="store_true", help="Start with GUI hidden (headless control)")
    args = parser.parse_args()

    root = get_runtime_root()
    launcher_log = root / "launcher.log"

    qt_app = QApplication(sys.argv)
    window = MainWindow()
    # Tray mode: closing GUI hides window; quit via tray menu.
    tray = setup_tray(qt_app, window, root)

    if args.hidden:
        window.hide()
    else:
        window.show()

    web_server = None
    influx_mgr = None

    try:
        # Start Web
        if not args.no_web:
            static_dir = root / "hv_test_system" / "web" / "static"
            # Hard fail early with clear diagnostics if resources are missing
            if not (static_dir / "index.html").exists():
                raise FileNotFoundError(f"Web UI not found: {static_dir} (missing index.html). "
                                        f"Ensure PyInstaller includes hv_test_system/web/static via --add-data.")
            app = create_app(window, static_dir=str(static_dir))
            web_log = root / "web.log" if getattr(sys, "frozen", False) else None
            web_server = UvicornServerThread(app, host=args.host, port=args.port, log_level="info", log_path=web_log)
            web_server.start()

        # Start InfluxDB
        if not args.no_influx:
            influx_mgr = InfluxDBManager(root, log_path=(root / "influx.log"))
            influx_mgr.start()

    except Exception as e:
        err = f"[Startup failed] {repr(e)}\n{traceback.format_exc()}"
        append_log(launcher_log, err)
        # GUI popup for user-friendly diagnostics (especially in -w mode).
        try:
            QMessageBox.critical(
                window,
                "启动失败",
                "程序启动过程中出现错误。\n\n"
                f"{e}\n\n"
                "已将详细信息写入 launcher.log。\n"
                "请确认打包时已包含静态资源与 monitoring 目录。",
            )
        except Exception:
            pass
        # Optional tray balloon (non-blocking)
        try:
            tray.showMessage("启动失败", str(e), QSystemTrayIcon.Critical, 8000)
        except Exception:
            pass

    def _shutdown():
        try:
            if web_server:
                web_server.stop(timeout_s=5.0)
        except Exception as e:
            append_log(launcher_log, f"[Web stop failed] {repr(e)}\n{traceback.format_exc()}")

        try:
            if influx_mgr:
                influx_mgr.stop()
        except Exception as e:
            append_log(launcher_log, f"[Influx stop failed] {repr(e)}\n{traceback.format_exc()}")

    qt_app.aboutToQuit.connect(_shutdown)
    sys.exit(qt_app.exec_())


if __name__ == "__main__":
    main()
