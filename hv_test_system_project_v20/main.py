import sys
from PyQt5.QtWidgets import QApplication
from hv_test_system import MainWindow

def main():
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    app.aboutToQuit.connect(window.closeEvent)
    sys.exit(app.exec_())

if __name__ == "__main__":
    main()
