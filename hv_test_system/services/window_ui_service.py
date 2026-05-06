from __future__ import annotations

from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import (
    QAction,
    QApplication,
    QHBoxLayout,
    QLabel,
    QMenu,
    QSplitter,
    QStatusBar,
    QStyle,
    QSystemTrayIcon,
    QWidget,
)

from ..chart_panel import ChartPanel
from ..control_panel import ControlPanel


class WindowUIService:
    """Build the main-window shell without embedding layout details in MainWindow."""

    APP_TITLE = "\u9ad8\u538b\u7535\u6e90\u4e0e\u4e07\u7528\u8868\u6d4b\u8bd5\u7cfb\u7edf"
    READY_MESSAGE = "\u7cfb\u7edf\u5c31\u7eea - \u8bf7\u8fde\u63a5\u8bbe\u5907\u5f00\u59cb\u6d4b\u8bd5"
    TRAY_TOOLTIP = APP_TITLE
    TRAY_SHOW_ACTION_TEXT = "\u663e\u793a\u4e3b\u7a97\u53e3"
    TRAY_QUIT_ACTION_TEXT = "\u9000\u51fa\u7a0b\u5e8f"
    WINDOW_GEOMETRY = (20, 20, 1260, 760)
    SPLITTER_SIZES = (480, 900)
    SPLITTER_MINIMUM_SIZE = (1380, 820)
    MAIN_STYLESHEET = """
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
            margin-top: 16px;
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
            top: 0px;
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
    """
    SPLITTER_STYLESHEET = """
        QSplitter::handle {
            background-color: #313244;
            width: 4px;
            border-radius: 2px;
            margin: 4px;
        }
        QSplitter::handle:hover {
            background-color: #89b4fa;
        }
    """

    def __init__(
        self,
        mw,
        widget_factory=None,
        splitter_factory=None,
        layout_factory=None,
        status_bar_factory=None,
        label_factory=None,
        control_panel_factory=None,
        chart_panel_factory=None,
        tray_icon_factory=None,
        menu_factory=None,
        action_factory=None,
        tray_available_getter=None,
    ):
        self.mw = mw
        self.widget_factory = widget_factory or QWidget
        self.splitter_factory = splitter_factory or QSplitter
        self.layout_factory = layout_factory or QHBoxLayout
        self.status_bar_factory = status_bar_factory or QStatusBar
        self.label_factory = label_factory or QLabel
        self.control_panel_factory = control_panel_factory or ControlPanel
        self.chart_panel_factory = chart_panel_factory or ChartPanel
        self.tray_icon_factory = tray_icon_factory or QSystemTrayIcon
        self.menu_factory = menu_factory or QMenu
        self.action_factory = action_factory or QAction
        self.tray_available_getter = tray_available_getter or QSystemTrayIcon.isSystemTrayAvailable

    def setup_ui(self):
        self._configure_window_chrome()
        self._build_main_layout()
        self._setup_status_bar()
        self._setup_tray_icon()
        self.mw.show_status_message(self.READY_MESSAGE)

    def _configure_window_chrome(self):
        self.mw.setWindowTitle(self.APP_TITLE)
        self.mw.setGeometry(*self.WINDOW_GEOMETRY)
        self.mw.setStyleSheet(self.MAIN_STYLESHEET)

    def _build_main_layout(self):
        main_widget = self.widget_factory()
        self.mw.setCentralWidget(main_widget)

        splitter = self._create_main_splitter()
        layout = self.layout_factory(main_widget)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(splitter)

        self.mw.main_widget = main_widget
        self.mw.main_splitter = splitter
        self.mw.main_layout = layout
        return main_widget

    def _create_main_splitter(self):
        splitter = self.splitter_factory(Qt.Horizontal)
        splitter.setHandleWidth(2)

        self.mw.control_panel = self.control_panel_factory(self.mw)
        splitter.addWidget(self.mw.control_panel)

        self.mw.chart_panel = self.chart_panel_factory(self.mw)
        splitter.addWidget(self.mw.chart_panel)

        splitter.setSizes(list(self.SPLITTER_SIZES))
        splitter.setStretchFactor(0, 0)
        splitter.setStretchFactor(1, 1)
        splitter.setMinimumSize(*self.SPLITTER_MINIMUM_SIZE)
        splitter.setStyleSheet(self.SPLITTER_STYLESHEET)
        return splitter

    def _setup_status_bar(self):
        status_bar = self.status_bar_factory()
        countdown_label = self.label_factory("")
        countdown_label.setObjectName("countdown")

        status_bar.addPermanentWidget(countdown_label)
        self.mw.setStatusBar(status_bar)

        self.mw.status_bar = status_bar
        self.mw.countdown_label = countdown_label

    def _setup_tray_icon(self):
        if not self._is_tray_available():
            self.mw.tray_icon = None
            return

        tray_icon = self._create_tray_icon()
        if tray_icon is None:
            self.mw.tray_icon = None
            return

        tray_menu = self._create_tray_menu()
        if tray_menu is not None:
            show_action = self._create_action(self.TRAY_SHOW_ACTION_TEXT, self._restore_window_from_tray)
            quit_action = self._create_action(self.TRAY_QUIT_ACTION_TEXT, self.mw.request_quit)
            if show_action is not None:
                tray_menu.addAction(show_action)
            if hasattr(tray_menu, "addSeparator"):
                tray_menu.addSeparator()
            if quit_action is not None:
                tray_menu.addAction(quit_action)
            self._safe_call_with_arg(tray_icon, "setContextMenu", tray_menu)
            self.mw.tray_menu = tray_menu
            self.mw.tray_show_action = show_action
            self.mw.tray_quit_action = quit_action

        self._safe_call_with_arg(tray_icon, "setToolTip", self.TRAY_TOOLTIP)
        self._connect_tray_activation(tray_icon)
        self._safe_call(tray_icon, "show")

        app = QApplication.instance()
        if app is not None:
            try:
                app.setQuitOnLastWindowClosed(False)
            except Exception:
                pass

        self.mw.tray_icon = tray_icon

    def _is_tray_available(self) -> bool:
        try:
            return bool(self.tray_available_getter())
        except Exception:
            return False

    def _create_tray_icon(self):
        icon = self._resolve_tray_icon()
        try:
            tray_icon = self.tray_icon_factory(self.mw)
        except TypeError:
            tray_icon = self.tray_icon_factory()
        except Exception:
            return None

        if icon is not None:
            self._safe_call_with_arg(tray_icon, "setIcon", icon)
            self._safe_call_with_arg(self.mw, "setWindowIcon", icon)
        return tray_icon

    def _resolve_tray_icon(self):
        try:
            icon = self.mw.windowIcon()
            if icon is not None and not icon.isNull():
                return icon
        except Exception:
            pass

        style = None
        try:
            style = self.mw.style()
        except Exception:
            pass
        if style is None:
            app = QApplication.instance()
            if app is not None:
                try:
                    style = app.style()
                except Exception:
                    style = None
        if style is None:
            return None

        try:
            return style.standardIcon(QStyle.SP_ComputerIcon)
        except Exception:
            return None

    def _create_tray_menu(self):
        try:
            return self.menu_factory(self.mw)
        except TypeError:
            try:
                return self.menu_factory()
            except Exception:
                return None
        except Exception:
            return None

    def _create_action(self, text: str, callback):
        try:
            action = self.action_factory(text, self.mw)
        except TypeError:
            try:
                action = self.action_factory(text)
            except Exception:
                return None
        except Exception:
            return None

        trigger = getattr(action, "triggered", None)
        if trigger is not None:
            try:
                trigger.connect(callback)
            except Exception:
                pass
        elif hasattr(action, "connect"):
            try:
                action.connect(callback)
            except Exception:
                pass
        return action

    def _connect_tray_activation(self, tray_icon):
        signal = getattr(tray_icon, "activated", None)
        if signal is None:
            return
        try:
            signal.connect(self._handle_tray_activation)
        except Exception:
            pass

    def _handle_tray_activation(self, reason):
        trigger_reason = getattr(QSystemTrayIcon, "Trigger", None)
        double_click_reason = getattr(QSystemTrayIcon, "DoubleClick", None)
        if reason in {trigger_reason, double_click_reason}:
            self._restore_window_from_tray()

    def _restore_window_from_tray(self):
        self._safe_call(self.mw, "showNormal")
        self._safe_call(self.mw, "show")
        self._safe_call(self.mw, "raise_")
        self._safe_call(self.mw, "activateWindow")

    def _safe_call(self, target, method_name: str):
        if target is None:
            return
        try:
            method = getattr(target, method_name)
        except Exception:
            return
        try:
            method()
        except Exception:
            pass

    def _safe_call_with_arg(self, target, method_name: str, value):
        if target is None:
            return
        try:
            method = getattr(target, method_name)
        except Exception:
            return
        try:
            method(value)
        except Exception:
            pass
