from __future__ import annotations

import csv
import os
import time
from datetime import datetime

from PyQt5.QtCore import QEventLoop, QTimer
from PyQt5.QtWidgets import QFileDialog

from ..record_models import RecordSample


class RecordingService:
    """Encapsulate record-path selection and CSV/SQLite/Influx recording flow."""

    START_RECORD_TEXT = "\u5f00\u59cb\u8bb0\u5f55"
    STOP_RECORD_TEXT = "\u505c\u6b62\u8bb0\u5f55"

    def __init__(self, mw):
        self.mw = mw

    def _current_csv_path(self) -> str:
        return str(self.mw.get_record_file_path() or "").strip()

    def _build_anode_min_payload(self):
        if not self.mw.all_anode_data:
            return None

        try:
            if self.mw.anode_min_value is not None:
                return {
                    "min_anode": self.mw.anode_min_value,
                    "voltage": self.mw.anode_min_voltage,
                    "time": self.mw.anode_min_time,
                }
        except Exception:
            pass

        try:
            min_anode = min(item[0] for item in self.mw.all_anode_data)
            min_data = next(item for item in self.mw.all_anode_data if item[0] == min_anode)
            return {"min_anode": min_anode, "voltage": min_data[1], "time": min_data[2]}
        except Exception:
            return None

    def _start_recording(self):
        csv_path = self._current_csv_path()
        if not csv_path:
            self.mw.log_message("Error: select a save path first")
            return False

        try:
            self._ensure_recording_can_start(csv_path)
            self._prepare_output_file(csv_path)
            self._configure_influx_bucket(csv_path)
            self._start_sqlite_run(csv_path)
            self._activate_recording_session(csv_path)
            self.mw.log_message("Recording started")
            try:
                self.mw.log_message(f"SQLite recording started: run={self.mw.current_run_id}")
            except Exception:
                pass
            return True
        except Exception as exc:
            self._reset_recording_ui_state()
            self.mw.log_message(f"Failed to start recording: {exc}")
            return False

    def _ensure_recording_can_start(self, csv_path: str):
        if getattr(self.mw, "is_converting", False):
            raise RuntimeError("Record post-processing is still running")
        self._validate_output_path(csv_path)

    def _validate_output_path(self, csv_path: str):
        clean_path = str(csv_path or "").strip()
        if not clean_path:
            raise ValueError("Output path is empty")
        if os.path.isdir(clean_path):
            raise IsADirectoryError(clean_path)

        parent_dir = os.path.abspath(os.path.dirname(clean_path) or ".")
        os.makedirs(parent_dir, exist_ok=True)
        if not os.access(parent_dir, os.W_OK):
            raise PermissionError(f"Output directory is not writable: {parent_dir}")

    def _reset_recording_ui_state(self):
        self.mw.is_recording = False
        try:
            self.mw.record_btn.setText(self.START_RECORD_TEXT)
        except Exception:
            pass

    def _prepare_output_file(self, csv_path: str):
        self._validate_output_path(csv_path)
        headers = list(self.mw._build_record_headers())
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as fh:
            csv.writer(fh).writerow(headers)

        try:
            self.mw.data_saver.headers = headers
        except Exception:
            pass
        try:
            self.mw.data_saver.set_output_path(csv_path)
        except Exception:
            pass

    def _configure_influx_bucket(self, csv_path: str):
        try:
            desired_bucket = self.mw.influx_writer.set_bucket_for_csv(csv_path, create_if_missing=True)
            config = getattr(getattr(self.mw, "influx_writer", None), "cfg", None)
            if not (config and bool(getattr(config, "enabled", False))):
                return
            actual_bucket = str(getattr(self.mw.influx_writer.cfg, "bucket", "") or "")
            error_text = str(getattr(self.mw.influx_writer, "bucket_create_error", "") or "")
            if actual_bucket == desired_bucket:
                self.mw.log_message(f"InfluxDB bucket switched: {desired_bucket}")
                return
            self.mw.log_message(
                f"InfluxDB bucket creation failed, falling back to {actual_bucket} (target {desired_bucket})"
            )
            if error_text:
                self.mw.log_message(f"Reason: {error_text}")
        except Exception as exc:
            config = getattr(getattr(self.mw, "influx_writer", None), "cfg", None)
            if config and bool(getattr(config, "enabled", False)):
                self.mw.log_message(f"InfluxDB bucket switch failed: {exc}")

    def _start_sqlite_run(self, csv_path: str):
        try:
            self.mw.current_run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
            self.mw.sqlite_recorder.start_run(
                self.mw.current_run_id,
                params={
                    "test_params": dict(self.mw.test_params),
                    "stabilization_params": dict(self.mw.stabilization_params),
                    "excel_path": csv_path,
                    "save_interval_s": float(self.mw.interval_edit.text() or 1),
                },
            )
        except Exception:
            pass

    def _activate_recording_session(self, _csv_path: str):
        interval_ms = int(self.mw.interval_edit.text()) * 1000
        self.mw.save_timer.start(interval_ms)
        self.mw.is_recording = True
        self.mw.record_btn.setText(self.STOP_RECORD_TEXT)
        self._reset_recording_buffers()

    def _reset_recording_buffers(self):
        self.mw.recorded_data.clear()
        self.mw.all_anode_data.clear()
        self.mw.anode_min_value = None
        self.mw.anode_min_voltage = None
        self.mw.anode_min_time = None
        self.mw.data_cache.clear()
        try:
            self.mw._last_cache_send_ts = time.time()
        except Exception:
            pass

    def _stop_recording(self):
        self._flush_recording_buffers_before_stop()
        self._deactivate_recording_session()
        self._finalize_background_conversion()
        self.mw.log_message("Recording stopped")
        try:
            self.mw._maybe_auto_cleanup_sqlite()
        except Exception:
            pass

    def _flush_recording_buffers_before_stop(self):
        self.mw.save_timer.stop()
        self.flush_data_cache(force=True)
        try:
            self.mw.data_saver.force_save()
        except Exception:
            pass

    def _deactivate_recording_session(self):
        self.mw.is_recording = False
        self.mw.record_btn.setText(self.START_RECORD_TEXT)
        try:
            self.mw.sqlite_recorder.stop_run()
        except Exception:
            pass
        self.mw.current_run_id = ""
        self.flush_data_cache(force=True)
        try:
            self.mw.data_saver.force_save()
        except Exception:
            pass

    def _finalize_background_conversion(self):
        csv_path = self._current_csv_path()
        if not os.path.exists(csv_path):
            return

        anode_min = None if self.mw.auto_recording else self._build_anode_min_payload()
        cycle_data = list(self.mw.cycle_data) if self.mw.cycle_data else None

        self.mw.is_converting = True
        try:
            self.mw.record_btn.setEnabled(False)
        except Exception:
            pass
        self.mw.log_message("Post-processing CSV summary/cycle data in the background...")
        self.mw.data_saver.request_convert(csv_path, anode_min=anode_min, cycle_data=cycle_data)

    def toggle_record(self):
        if self.mw.is_recording:
            self._stop_recording()
            return True
        return self._start_recording()

    def save_data(self):
        if not self._should_collect_sample():
            return

        try:
            sample = self._build_live_sample()
            self._enqueue_sample(sample)
            self._update_record_history(sample)
            self._update_anode_minimum(sample)
            self._update_cycle_history(sample)
            self._maybe_flush_cache()
        except Exception as exc:
            self.mw.log_message(f"Failed to prepare recording data: {exc}")

    def _should_collect_sample(self) -> bool:
        if not self.mw.is_recording:
            return False
        if self.mw.is_cycle_testing and not self.mw.cycle_recording_active:
            return False
        return True

    def _build_live_sample(self) -> RecordSample:
        current_time = datetime.now()
        current_time_str = current_time.strftime("%Y-%m-%d %H:%M:%S")
        test_power_name, stab_power_name = self._resolve_record_power_names()
        test_power_voltage, stab_power_voltage = self._resolve_record_power_voltages(
            test_power_name,
            stab_power_name,
        )
        cathode_state, gate_state, anode_state, backup_state, vacuum_val = self._read_meter_values()
        return self._build_record_sample(
            current_time_str=current_time_str,
            test_power_name=test_power_name,
            test_power_voltage=test_power_voltage,
            cathode_state=cathode_state,
            gate_state=gate_state,
            anode_state=anode_state,
            backup_state=backup_state,
            vacuum_val=vacuum_val,
            stab_power_name=stab_power_name,
            stab_power_voltage=stab_power_voltage,
        )

    def _resolve_record_power_names(self):
        return (
            self.mw._get_record_power_name("test"),
            self.mw._get_record_power_name("stabilization"),
        )

    def _resolve_record_power_voltages(self, test_power_name: str, stab_power_name: str):
        test_power_voltage = self.mw._get_record_power_voltage(test_power_name)
        stab_power_voltage = self.mw._get_record_power_voltage(stab_power_name)
        return (
            0.0 if test_power_voltage is None else test_power_voltage,
            0.0 if stab_power_voltage is None else stab_power_voltage,
        )

    def _read_meter_values(self):
        self.mw.data_mutex.lock()
        try:
            return (
                dict(self.mw.meter_data["cathode"]),
                dict(self.mw.meter_data["gate"]),
                dict(self.mw.meter_data["anode"]),
                dict(self.mw.meter_data["backup"]),
                self.mw.meter_data.get("vacuum", {}).get("value", 0.0),
            )
        finally:
            self.mw.data_mutex.unlock()

    def _enqueue_sample(self, sample: RecordSample):
        try:
            self.mw.influx_writer.enqueue(
                fields=sample.to_influx_fields(),
                tags=sample.to_influx_tags(),
                timestamp_ns=sample.timestamp_ns,
            )
        except Exception:
            pass

        try:
            self.mw.sqlite_recorder.enqueue_row(
                ts_ms=sample.timestamp_ms,
                row=sample.to_sqlite_row(),
            )
        except Exception:
            pass

    def _update_record_history(self, sample: RecordSample):
        csv_row = sample.to_csv_row()
        self.mw.data_cache.append(csv_row)
        self.mw.recorded_data.append(csv_row)
        self.mw.all_anode_data.append((sample.anode, sample.test_power_voltage, sample.time_text))

    def _update_anode_minimum(self, sample: RecordSample):
        try:
            if self.mw.anode_min_value is None or sample.anode < self.mw.anode_min_value:
                self.mw.anode_min_value = sample.anode
                self.mw.anode_min_voltage = sample.test_power_voltage
                self.mw.anode_min_time = sample.time_text
        except Exception:
            pass

    def _update_cycle_history(self, sample: RecordSample):
        if self.mw.is_cycle_testing and self.mw.is_recording:
            self.mw.current_cycle_anode_data.append(
                (sample.anode, sample.test_power_voltage, sample.time_text)
            )

    def _maybe_flush_cache(self):
        now_ts = time.time()
        if len(self.mw.data_cache) >= self.mw.cache_size:
            self.flush_data_cache()
            return
        cache_send_interval = float(getattr(self.mw, "cache_send_interval", 1.0))
        last_send_ts = float(getattr(self.mw, "_last_cache_send_ts", 0.0))
        if now_ts - last_send_ts >= cache_send_interval:
            self.flush_data_cache()

    def _build_record_sample(
        self,
        *,
        current_time_str: str,
        test_power_name: str,
        test_power_voltage: float,
        cathode_state: dict,
        gate_state: dict,
        anode_state: dict,
        backup_state: dict,
        vacuum_val: float,
        stab_power_name: str,
        stab_power_voltage: float,
    ) -> RecordSample:
        test_power_device = self.mw._find_power_device(test_power_name) or {}
        stab_power_device = self.mw._find_power_device(stab_power_name) or {}
        return RecordSample.from_measurements(
            time_text=current_time_str,
            test_power_name=test_power_name,
            test_power_voltage=test_power_voltage,
            cathode=float(cathode_state.get("value", 0.0) or 0.0),
            cathode_unit=str(cathode_state.get("unit", "") or ""),
            gate=float(gate_state.get("value", 0.0) or 0.0),
            gate_unit=str(gate_state.get("unit", "") or ""),
            anode=float(anode_state.get("value", 0.0) or 0.0),
            anode_unit=str(anode_state.get("unit", "") or ""),
            backup=float(backup_state.get("value", 0.0) or 0.0),
            backup_unit=str(backup_state.get("unit", "") or ""),
            vacuum=vacuum_val,
            stabilization_power_name=stab_power_name,
            stabilization_power_voltage=stab_power_voltage,
            test_power_type=str(test_power_device.get("type", "")),
            stabilization_power_type=str(stab_power_device.get("type", "")),
            session_id=str(self.mw.session_id),
            run_id=str(self.mw.current_run_id or ""),
            is_testing=bool(self.mw.is_testing),
            is_stabilizing=bool(self.mw.is_stabilizing),
            is_recording=bool(self.mw.is_recording),
            timestamp_ns=time.time_ns(),
        )

    def flush_data_cache(self, force: bool = False):
        if not self.mw.data_cache:
            return
        if (not self.mw.is_recording) and (not force):
            return

        try:
            rows_to_send = list(self.mw.data_cache)
            self.mw.data_cache.clear()
            self.mw.data_saver.add_batch(rows_to_send)
            try:
                self.mw._last_cache_send_ts = time.time()
            except Exception:
                pass
        except Exception as exc:
            print(f"Failed to flush data cache: {exc}")

    def calculate_and_save_anode_min(self):
        try:
            anode_min = self._build_anode_min_payload()
            if anode_min is None:
                self.mw.log_message("No anode data available for minimum calculation")
                return

            csv_path = self._current_csv_path()
            if csv_path:
                self.mw.data_saver.request_convert(
                    csv_path,
                    anode_min=anode_min,
                    cycle_data=list(self.mw.cycle_data) if self.mw.cycle_data else None,
                )
                self.mw.log_message(
                    f"Anode minimum saved: value={anode_min['min_anode']:.4f}, "
                    f"voltage={anode_min['voltage']}, time={anode_min['time']}"
                )
        except Exception as exc:
            self.mw.log_message(f"Failed to calculate anode minimum: {exc}")

    def save_recorded_data(self):
        if not self.mw.recorded_data:
            return

        try:
            csv_path = self._current_csv_path()
            if csv_path:
                self.mw.data_saver.request_convert(
                    csv_path,
                    anode_min=None,
                    cycle_data=list(self.mw.cycle_data) if self.mw.cycle_data else None,
                )
                if self.mw.cycle_data:
                    self.mw.log_message(
                        f"Generated cycle data for {len(self.mw.cycle_data)} cycles"
                    )
            self.mw.log_message("Recorded data finalized")
        except Exception as exc:
            self.mw.log_message(f"Failed to finalize recorded data: {exc}")

    def select_path(self):
        try:
            default_path = self._default_save_path()
            path = QFileDialog.getSaveFileName(
                self.mw,
                "Save File",
                default_path,
                "CSV Files (*.csv)",
            )[0]

            if path:
                self.mw.set_record_file_path(path)
                self.mw.log_message(f"Record save path: {path}")
                self.mw.save_config_from_ui()
        except Exception as exc:
            self.mw.log_message(f"Failed to choose save path: {exc}")

    def _default_save_path(self) -> str:
        default_path = self._current_csv_path()
        if not default_path and self.mw.config.has_option("DataRecord", "save_path"):
            default_path = self.mw.config.get("DataRecord", "save_path")
        if default_path:
            return default_path
        return f"test_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    def finalize_on_shutdown(self):
        if not self.mw.is_recording:
            return

        self.flush_data_cache(force=True)
        try:
            self.mw.data_saver.force_save()
        except Exception:
            pass

        csv_path = self._current_csv_path()
        if not os.path.exists(csv_path):
            return

        anode_min = None if self.mw.auto_recording else self._build_anode_min_payload()
        cycle_data = list(self.mw.cycle_data) if self.mw.cycle_data else None

        self.mw.is_converting = True
        self.mw.log_message("Finalizing CSV summary/cycle data before shutdown...")
        self.mw.data_saver.request_convert(csv_path, anode_min=anode_min, cycle_data=cycle_data)

        try:
            loop = QEventLoop()
            timer = QTimer()
            timer.setSingleShot(True)
            timer.timeout.connect(loop.quit)
            self.mw.data_saver.convert_complete.connect(loop.quit)
            timer.start(120000)
            loop.exec_()
            timer.stop()
        except Exception:
            pass
