import re

FILE_PATH = 'e:/application/hv_test_system_project_v2.10.3/hv_test_system/services/window_ui_service.py'

LIGHT_THEME = """    MAIN_STYLESHEET = \"\"\"
        QMainWindow, QDialog, QMessageBox {
            background-color: #f5f5f7;
        }
        QWidget {
            font-family: 'Segoe UI', 'Microsoft YaHei', sans-serif;
            font-size: 9pt;
            color: #1d1d1f;
        }
        QGroupBox {
            font-weight: bold;
            border: 1px solid #d2d2d7;
            border-radius: 8px;
            margin-top: 20px;
            padding-top: 16px;
            background-color: #ffffff;
            color: #0066cc;
        }
        QGroupBox::title {
            subcontrol-origin: margin;
            subcontrol-position: top left;
            padding: 4px 12px;
            background-color: #e8f0fe;
            color: #1a73e8;
            border: 1px solid #d2d2d7;
            border-radius: 6px;
            font-size: 9pt;
            left: 12px;
            top: -12px;
        }
        QPushButton {
            background-color: #ffffff;
            color: #1d1d1f;
            border: 1px solid #d2d2d7;
            border-radius: 5px;
            padding: 6px 12px;
            min-height: 22px;
            min-width: 60px;
            font-weight: bold;
            font-size: 9pt;
        }
        QPushButton:hover {
            background-color: #f0f4f8;
            border: 1px solid #1a73e8;
            color: #1a73e8;
        }
        QPushButton:pressed {
            background-color: #e8f0fe;
            border: 1px solid #174ea6;
        }
        QPushButton:disabled {
            background-color: #f5f5f7;
            color: #86868b;
            border: 1px solid #e5e5ea;
        }
        QLineEdit, QComboBox, QSpinBox, QDoubleSpinBox {
            padding: 4px 8px;
            border: 1px solid #d2d2d7;
            border-radius: 5px;
            background-color: #ffffff;
            color: #1d1d1f;
            font-size: 9pt;
            min-height: 22px;
        }
        QLineEdit:focus, QComboBox:focus, QSpinBox:focus, QDoubleSpinBox:focus {
            border: 1px solid #1a73e8;
            background-color: #ffffff;
        }
        QComboBox::drop-down {
            border: none;
            width: 20px;
        }
        QComboBox QAbstractItemView {
            background-color: #ffffff;
            border: 1px solid #d2d2d7;
            color: #1d1d1f;
            selection-background-color: #e8f0fe;
            selection-color: #1a73e8;
        }
        QTextEdit, QPlainTextEdit {
            border: 1px solid #d2d2d7;
            border-radius: 5px;
            background-color: #ffffff;
            color: #1d1d1f;
            font-family: Consolas, 'Courier New', monospace;
            font-size: 9pt;
            padding: 4px;
        }
        QLabel#titleLabel {
            font-size: 15pt;
            font-weight: bold;
            padding: 10px;
            background-color: qlineargradient(x1:0, y1:0, x2:1, y2:0, stop:0 #1a73e8, stop:1 #4285f4);
            color: #ffffff;
            border-radius: 6px;
        }
        QLabel#chartTitle {
            font-size: 11pt;
            font-weight: bold;
            padding: 6px;
            background-color: #e8f0fe;
            color: #1a73e8;
            border-radius: 4px;
            border: 1px solid #1a73e8;
        }
        QLabel#voltageLabel {
            font-size: 12pt;
            font-weight: bold;
            color: #d93025;
            padding: 4px;
            background-color: #fce8e6;
            border: 1px solid #ea4335;
            border-radius: 4px;
            min-width: 80px;
        }
        QLabel#meterValue {
            font-size: 9pt;
            padding: 3px 6px;
            background-color: #e6f4ea;
            border: 1px solid #34a853;
            border-radius: 4px;
            min-width: 70px;
            font-weight: bold;
            color: #188038;
        }
        QLabel#pathLabel {
            background-color: #fff3e0;
            padding: 4px 6px;
            font-size: 8pt;
            border: 1px solid #ffb74d;
            border-radius: 4px;
            color: #e65100;
        }
        QLabel#countdown {
            font-size: 10pt;
            font-weight: bold;
            color: #bf360c;
            padding: 3px 6px;
            background-color: #fbe9e7;
            border: 1px solid #d84315;
            border-radius: 4px;
        }
        QLabel#settingsLabel {
            background-color: #e3f2fd;
            color: #1565c0;
            padding: 4px 6px;
            font-size: 8pt;
            border: 1px solid #90caf9;
            border-radius: 4px;
        }
        QStatusBar {
            background-color: #f5f5f7;
            color: #5f6368;
            font-size: 9pt;
            border-top: 1px solid #d2d2d7;
        }
        QScrollArea {
            border: none;
            background-color: transparent;
        }
        QScrollBar:vertical {
            border: none;
            background: #f5f5f7;
            width: 10px;
            margin: 0px 0px 0px 0px;
        }
        QScrollBar::handle:vertical {
            background: #dadce0;
            min-height: 30px;
            border-radius: 5px;
        }
        QScrollBar::handle:vertical:hover {
            background: #bdc1c6;
        }
        QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
            height: 0px;
        }
        QScrollBar:horizontal {
            border: none;
            background: #f5f5f7;
            height: 10px;
            margin: 0px 0px 0px 0px;
        }
        QScrollBar::handle:horizontal {
            background: #dadce0;
            min-width: 30px;
            border-radius: 5px;
        }
        QScrollBar::handle:horizontal:hover {
            background: #bdc1c6;
        }
        QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {
            width: 0px;
        }
        QMenuBar {
            background-color: #ffffff;
            color: #1d1d1f;
            border-bottom: 1px solid #d2d2d7;
            font-size: 10pt;
            padding: 2px;
        }
        QMenuBar::item {
            background: transparent;
            padding: 6px 12px;
            margin: 1px 2px;
            border-radius: 4px;
        }
        QMenuBar::item:selected {
            background-color: #e8f0fe;
            color: #1a73e8;
        }
        QMenu {
            background-color: #ffffff;
            color: #1d1d1f;
            border: 1px solid #d2d2d7;
            border-radius: 4px;
        }
        QMenu::item {
            padding: 6px 28px 6px 18px;
        }
        QMenu::item:selected {
            background-color: #e8f0fe;
            color: #1a73e8;
        }
        QMenu::separator {
            height: 1px;
            background: #d2d2d7;
            margin: 4px 8px;
        }
        QCheckBox {
            color: #1d1d1f;
        }
        QCheckBox::indicator {
            width: 16px;
            height: 16px;
            border-radius: 4px;
            border: 1px solid #d2d2d7;
            background-color: #ffffff;
        }
        QCheckBox::indicator:checked {
            background-color: #1a73e8;
            border: 1px solid #1a73e8;
        }
        QCheckBox::indicator:hover {
            border: 1px solid #1a73e8;
        }
        
        QTabWidget::pane {
            border: 1px solid #d2d2d7;
            border-radius: 6px;
            background-color: #ffffff;
            margin-top: -1px;
        }
        QTabBar::tab {
            background-color: #f5f5f7;
            color: #5f6368;
            border: 1px solid #d2d2d7;
            border-bottom: none;
            border-top-left-radius: 6px;
            border-top-right-radius: 6px;
            padding: 8px 16px;
            margin-right: 2px;
            font-weight: bold;
        }
        QTabBar::tab:selected {
            background-color: #ffffff;
            color: #1a73e8;
            border-bottom: 2px solid #1a73e8;
        }
        QTabBar::tab:hover:!selected {
            background-color: #e8f0fe;
            color: #1a73e8;
        }
    \"\"\""""

with open(FILE_PATH, 'r', encoding='utf-8') as f:
    content = f.read()

content = re.sub(r'    MAIN_STYLESHEET = """(.*?)"""', LIGHT_THEME, content, flags=re.DOTALL)

with open(FILE_PATH, 'w', encoding='utf-8') as f:
    f.write(content)
