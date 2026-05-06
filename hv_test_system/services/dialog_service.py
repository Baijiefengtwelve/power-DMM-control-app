from __future__ import annotations

from ..ui_dialogs import (
    MeterSettingsDialog,
    PowerSettingsDialog,
    RemoteInfluxSettingsDialog,
    VacuumSettingsDialog,
)


class DialogService:
    """Own simple modal dialog launchers used by the main window and menus."""

    def __init__(self, mw, dialog_factories=None):
        self.mw = mw
        self.dialog_factories = dialog_factories or {
            "meter": MeterSettingsDialog,
            "vacuum": VacuumSettingsDialog,
            "power": PowerSettingsDialog,
            "remote_influx": RemoteInfluxSettingsDialog,
        }

    def show_meter_settings_dialog(self):
        return self._show_dialog("meter")

    def show_vacuum_settings_dialog(self):
        return self._show_dialog("vacuum")

    def show_power_settings_dialog(self):
        return self._show_dialog("power")

    def show_remote_influx_settings_dialog(self):
        return self._show_dialog("remote_influx")

    def _show_dialog(self, key: str):
        dialog_cls = self.dialog_factories[key]
        dialog = dialog_cls(self.mw)
        return dialog.exec_()
