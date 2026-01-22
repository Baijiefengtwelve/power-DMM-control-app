import argparse
import sys
import threading

from PyQt5.QtWidgets import QApplication

from hv_test_system import MainWindow
from hv_test_system.web import create_app


def run_uvicorn(app, host: str, port: int):
    import uvicorn
    uvicorn.run(app, host=host, port=port, log_level="info")


def main():
    parser = argparse.ArgumentParser(description="HV Test System - Web Mode")
    parser.add_argument("--host", default="0.0.0.0", help="Web bind host (default: 0.0.0.0)")
    parser.add_argument("--port", default=8000, type=int, help="Web port (default: 8000)")
    parser.add_argument("--gui", action="store_true", help="Show the desktop GUI window")
    args = parser.parse_args()

    qt_app = QApplication(sys.argv)
    window = MainWindow()

    if args.gui:
        window.show()
    else:
        # Run headless (window hidden) but keep Qt event loop alive
        window.hide()

    app = create_app(window)

    t = threading.Thread(target=run_uvicorn, args=(app, args.host, args.port), daemon=True)
    t.start()

    sys.exit(qt_app.exec_())


if __name__ == "__main__":
    main()
