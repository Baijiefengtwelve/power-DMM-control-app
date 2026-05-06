from __future__ import annotations

from collections import deque

from PyQt5.QtCore import QMutex, QTimer

from ..data_buffer import DataBuffer
from ..haps06_controller import HAPS06Controller
from ..keithley_controller import Keithley248Controller
from ..thread_runtime import CountdownManager, DataSaver


class BootstrapService:
    """Initialize controllers and runtime buffers before the app starts servicing UI actions."""

    def __init__(self, mw):
        self.mw = mw

    def setup_controllers(self):
        self._setup_power_controllers()
        self._setup_meter_runtime()
        self._setup_data_pipeline()
        self._setup_recording_runtime()
        self._setup_test_runtime()
        self._setup_update_runtime()

    def _setup_power_controllers(self):
        self.mw.hv_controller = HAPS06Controller()
        # Avoid cross-thread UI updates directly from the hardware controller.
        self.mw.hv_controller.voltage_update_callback = None
        self.mw.hv_voltage_poller = None
        self.mw._hv_v_cache = 0.0
        self.mw._hv_v_ts = 0.0
        self.mw.keithley_controller = Keithley248Controller()

    def _setup_meter_runtime(self):
        self.mw.meter_threads = {}
        self.mw.meter_data = {
            "cathode": {"value": 0.0, "unit": "", "kind": "", "coefficient": 1.0, "timestamp": 0.0, "valid": False},
            "gate": {"value": 0.0, "unit": "", "kind": "", "coefficient": 1.0, "timestamp": 0.0, "valid": False},
            "anode": {"value": 0.0, "unit": "", "kind": "", "coefficient": 1.0, "timestamp": 0.0, "valid": False},
            "backup": {"value": 0.0, "unit": "", "kind": "", "coefficient": 1.0, "timestamp": 0.0, "valid": False},
            "vacuum": {"value": 0.0, "unit": "Pa", "kind": "vacuum", "coefficient": 1.0, "timestamp": 0.0, "valid": False},
        }
        self.mw.data_mutex = QMutex()
        plot_limit = self.mw.config.get("PlotSettings", "max_points", fallback="0")
        self.mw.data_buffer = DataBuffer(max_points=plot_limit)

    def _setup_data_pipeline(self):
        self.mw.data_saver = DataSaver()
        self.mw.data_saver.save_complete.connect(self.mw.on_data_saved)
        self.mw.data_saver.convert_complete.connect(self.mw.on_data_converted)
        self.mw.data_saver.start()

    def _setup_recording_runtime(self):
        self.mw.cycle_data = []
        self.mw.current_cycle_anode_data = []
        self.mw.recorded_data = deque(maxlen=10000)
        self.mw.all_anode_data = deque(maxlen=10000)
        self.mw.anode_min_value = None
        self.mw.anode_min_voltage = None
        self.mw.anode_min_time = None
        self.mw.data_cache = []
        self.mw.cache_size = 50
        self.mw.cache_send_interval = 1.0
        self.mw._last_cache_send_ts = 0.0
        self.mw.last_test_run_summary = ""
        self.mw.save_timer = QTimer()
        self.mw.save_timer.timeout.connect(self.mw.save_data)
        self.mw.excel_file = None
        self.mw.is_recording = False
        self.mw.is_converting = False

    def _setup_test_runtime(self):
        self.mw.is_testing = False
        self.mw.is_cycle_testing = False
        self.mw.current_cycle = 0
        self.mw.test_mode = "升压"
        self.mw.auto_recording = False
        self.mw.cycle_recording_active = False
        self.mw.is_stabilizing = False
        self.mw.stabilization_thread = None
        self.mw.active_test_controller = None
        self.mw.active_test_power_source = None
        self.mw.active_stabilization_controller = None
        self.mw.active_stabilization_power_source = None
        self.mw.test_run_started_at = 0.0
        self.mw.test_run_source_name = ""
        self.mw.test_run_cycle_mode = False
        self.mw._test_run_stabilization_baseline = {"completion": 0, "failure": 0}
        self.mw.stabilization_completion_count = 0
        self.mw.stabilization_failure_count = 0
        self.mw._stabilization_last_completed = False
        self.mw.countdown_manager = CountdownManager(self.mw.update_countdown_display)

    def _setup_update_runtime(self):
        self.mw.data_update_queue = []
        self.mw.last_meter_update_time = 0
        self.mw.meter_update_interval = 0.5
