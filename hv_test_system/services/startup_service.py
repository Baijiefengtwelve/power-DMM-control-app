from __future__ import annotations

from ..config_manager import ConfigManager
from ..monitoring.influx_writer import InfluxWriter
from ..parameter_models import StabilizationParameters, TestParameters
from ..sqlite_maintenance import load_retention_from_config
from ..sqlite_recorder import SQLiteRecorder
from .service_registry import ServiceRegistry


class StartupService:
    """Coordinate main-window startup without embedding the whole sequence in MainWindow."""

    TEST_PARAM_DEFAULTS = {
        "start_voltage": 0,
        "target_voltage": 1000,
        "voltage_step": 10,
        "step_delay": 1,
        "cycle_time": 10,
        "power_source_name": "自动判断",
        "power_source": "auto",
    }
    STABILIZATION_PARAM_DEFAULTS = {
        "target_current": 1000,
        "stability_range": 5,
        "start_voltage": 100,
        "power_source_name": "自动判断",
        "power_source": "auto",
        "current_source": "keithley",
        "adjust_frequency": 1,
        "max_adjust_voltage": 50,
        "algorithm": "pid",
        "pid_kp": 0.05,
        "pid_ki": 0.01,
        "pid_kd": 0.0,
    }

    def __init__(self, mw, service_registry_factory=None):
        self.mw = mw
        self.service_registry_factory = service_registry_factory or ServiceRegistry

    def bootstrap_main_window(self):
        self.initialize_runtime_defaults()
        self.register_services()
        self.mw.setup_ui()
        self.mw.setup_menu_bar()
        self.mw.setup_controllers()
        self.start_background_services()
        self.finalize_startup()

    def initialize_runtime_defaults(self):
        self.mw._allow_quit = False
        self.mw.tray_icon = None
        self.mw.config_manager = ConfigManager()
        self.mw.config = self.mw.config_manager.load_config()
        self.mw.influx_writer = InfluxWriter.from_config(self.mw.config)
        self.mw.sqlite_recorder = SQLiteRecorder.from_config(self.mw.config)
        self.mw.retention_policy = load_retention_from_config(self.mw.config)
        self.mw.power_devices = []
        self.mw.connected_named_power_controllers = {}
        self.mw.named_keithley_controllers = {}
        self.mw.remote_control_config = {"host": "127.0.0.1", "port": 8000}
        self.mw.test_params = TestParameters(dict(self.TEST_PARAM_DEFAULTS))
        self.mw.stabilization_params = StabilizationParameters(dict(self.STABILIZATION_PARAM_DEFAULTS))
        self.mw.vacuum_alarm_max_pa = "1e-3"

    def register_services(self):
        registry = self.service_registry_factory(self.mw)
        self.mw.service_registry = registry
        return registry.register_services()

    def start_background_services(self):
        self._safe_call(getattr(self.mw, "influx_writer", None), "start")
        self._safe_call(getattr(self.mw, "sqlite_recorder", None), "start")

    def finalize_startup(self):
        self.mw.setup_timers()
        self.mw.refresh_all_ports()
        self.mw.load_config_to_ui()
        self.mw.update_settings_display()
        self._safe_call(self.mw, "update_power_summary_label")
        self.mw.update_power_action_buttons()

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
