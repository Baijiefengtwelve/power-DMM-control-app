from __future__ import annotations

from PyQt5.QtWidgets import QAction, QMenuBar


class MenuService:
    """Own menu-bar construction and persistent action references."""

    MENU_ACTIONS = (
        ("meter_open", "万用表", "打开万用表设置", "show_meter_settings_dialog"),
        ("vacuum_open", "真空计", "打开真空计设置", "show_vacuum_settings_dialog"),
        ("power_open", "电源", "打开电源设置", "show_power_settings_dialog"),
        ("remote_open", "远程控制 / InfluxDB", "打开远程控制 / InfluxDB 设置", "show_remote_influx_settings_dialog"),
        ("plot_open", "图线设置", "打开图线设置", "show_plot_settings"),
        ("refresh_ports", "工具", "刷新端口列表", "refresh_all_ports"),
    )

    def __init__(self, mw, menu_bar_factory=None, action_factory=None):
        self.mw = mw
        self.menu_bar_factory = menu_bar_factory or (lambda parent: QMenuBar(parent))
        self.action_factory = action_factory or (lambda text, parent: QAction(text, parent))

    def setup_menu_bar(self):
        menu_bar = self.menu_bar_factory(self.mw)
        try:
            menu_bar.setNativeMenuBar(False)
        except Exception:
            pass
        self.mw.setMenuBar(menu_bar)
        try:
            menu_bar.clear()
        except Exception:
            pass
        try:
            menu_bar.setVisible(True)
        except Exception:
            pass

        actions = {}
        for action_key, menu_title, action_text, handler_name in self.MENU_ACTIONS:
            menu = menu_bar.addMenu(menu_title)
            action = self.action_factory(action_text, self.mw)
            action.triggered.connect(getattr(self.mw, handler_name))
            menu.addAction(action)
            actions[action_key] = action

        self.mw._menu_actions = actions
        return menu_bar
