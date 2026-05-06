from __future__ import annotations

from collections import deque
import math
import threading
import time


class DataBuffer:
    """Telemetry buffer with optional point retention limit."""

    SERIES_COUNT = 9
    SERIES_KEYS = (
        "time_data",
        "cathode_data",
        "gate_data",
        "anode_data",
        "backup_data",
        "keithley_voltage_data",
        "vacuum_data",
        "gate_plus_anode_data",
        "anode_cathode_ratio_data",
    )

    def __init__(self, max_points=None):
        self.max_points = self.normalize_max_points(max_points)
        self.start_time = time.time()
        self.last_plot_update = 0.0
        self.plot_update_interval = 0.2
        self._lock = threading.RLock()
        self._revision = 0
        self._cached_plot_revision = -1
        self._cached_plot_data = None
        self._allocate_storage()

    @staticmethod
    def normalize_max_points(value):
        if value in (None, "", 0, "0"):
            return None
        try:
            numeric = int(float(value))
        except Exception:
            return None
        return numeric if numeric > 0 else None

    @property
    def revision(self):
        with self._lock:
            return self._revision

    def _allocate_storage(self):
        for key in self.SERIES_KEYS:
            setattr(self, key, deque(maxlen=self.max_points))

    def _invalidate_cache(self):
        self._revision += 1
        self._cached_plot_revision = -1
        self._cached_plot_data = None

    def _append_value(self, key: str, value):
        getattr(self, key).append(float(value))

    def add_data(
        self,
        cathode,
        gate,
        anode,
        backup,
        keithley_voltage,
        vacuum,
        *,
        meter_kinds=None,
    ):
        current_time = time.time() - self.start_time
        meter_kinds = dict(meter_kinds or {})

        gate_plus_anode = self._combine_series(
            (gate, anode, backup),
            (
                meter_kinds.get("gate"),
                meter_kinds.get("anode"),
                meter_kinds.get("backup"),
            ),
        )
        ratio = self._compute_ratio(
            cathode,
            anode,
            meter_kinds.get("cathode"),
            meter_kinds.get("anode"),
        )

        with self._lock:
            self._append_value("time_data", current_time)
            self._append_value("cathode_data", cathode)
            self._append_value("gate_data", gate)
            self._append_value("anode_data", anode)
            self._append_value("backup_data", backup)
            self._append_value("keithley_voltage_data", keithley_voltage)
            self._append_value("vacuum_data", vacuum)
            self._append_value("gate_plus_anode_data", gate_plus_anode)
            self._append_value("anode_cathode_ratio_data", ratio)

            self._trim_if_needed()
            self._invalidate_cache()

    def _trim_if_needed(self):
        return

    @staticmethod
    def _combine_series(values, kinds) -> float:
        kind_set = {str(kind or "").strip().lower() for kind in kinds if str(kind or "").strip()}
        if not kind_set:
            return float(sum(float(v) for v in values))
        if len(kind_set) != 1:
            return math.nan
        only_kind = next(iter(kind_set))
        if only_kind not in {"voltage", "current"}:
            return math.nan
        return float(sum(float(v) for v in values))

    @staticmethod
    def _compute_ratio(cathode, anode, cathode_kind, anode_kind) -> float:
        left_kind = str(cathode_kind or "").strip().lower()
        right_kind = str(anode_kind or "").strip().lower()
        if left_kind and right_kind and left_kind != right_kind:
            return math.nan
        cathode_value = float(cathode)
        anode_value = float(anode)
        if cathode_value == 0.0:
            return math.nan
        return (anode_value / cathode_value) * 100.0

    def get_plot_data(self):
        with self._lock:
            if not self.time_data:
                return tuple([] for _ in range(self.SERIES_COUNT))

            if self._cached_plot_revision == self._revision and self._cached_plot_data is not None:
                return self._cached_plot_data

            plot_data = tuple(tuple(getattr(self, key)) for key in self.SERIES_KEYS)
            self._cached_plot_revision = self._revision
            self._cached_plot_data = plot_data
            return plot_data

    def reconfigure(self, *, max_points=None):
        new_limit = self.normalize_max_points(max_points)
        with self._lock:
            if new_limit == self.max_points:
                return
            existing = {key: tuple(getattr(self, key)) for key in self.SERIES_KEYS}
            self.max_points = new_limit
            self._allocate_storage()
            for key, values in existing.items():
                series = getattr(self, key)
                for value in values:
                    series.append(value)
            self._trim_if_needed()
            self._invalidate_cache()

    def clear(self):
        with self._lock:
            self._allocate_storage()
            self.start_time = time.time()
            self._invalidate_cache()
