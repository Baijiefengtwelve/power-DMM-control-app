import serial.tools.list_ports

from PyQt5.QtWidgets import QMainWindow
from .runtime_state import AppRuntimeState, RuntimeStateField

from .parameter_models import (
    AUTO_POWER_SOURCE_NAME,
)
from .services import (
    StartupService,
)


def _require_registered_service(window, attr_name: str):
    service = getattr(window, attr_name, None)
    if service is None:
        raise RuntimeError(
            f"{attr_name} is not available. Ensure StartupService.register_services() "
            "completed before using MainWindow service delegates."
        )
    return service


class MainWindow(QMainWindow):
    session_id = RuntimeStateField("session_id")
    current_run_id = RuntimeStateField("current_run_id")

    _prev_testing = RuntimeStateField("prev_testing", bool)
    _prev_stabilizing = RuntimeStateField("prev_stabilizing", bool)
    _prev_recording = RuntimeStateField("prev_recording", bool)

    _hv_v_cache = RuntimeStateField("hv_voltage_cache", float)
    _hv_v_ts = RuntimeStateField("hv_voltage_ts", float)
    _keithley_v_cache = RuntimeStateField("keithley_voltage_cache", float)
    _keithley_v_ts = RuntimeStateField("keithley_voltage_ts", float)
    power_voltage_cache = RuntimeStateField("power_voltage_cache")
    power_voltage_ts = RuntimeStateField("power_voltage_ts")

    connected_power_name_by_type = RuntimeStateField("connected_power_name_by_type")
    pending_haps06_power_name = RuntimeStateField("pending_haps06_power_name")
    save_path = RuntimeStateField("save_path")
    _service_runtime = RuntimeStateField("service_runtime")

    is_recording = RuntimeStateField("is_recording", bool)
    is_converting = RuntimeStateField("is_converting", bool)
    is_testing = RuntimeStateField("is_testing", bool)
    is_cycle_testing = RuntimeStateField("is_cycle_testing", bool)
    current_cycle = RuntimeStateField("current_cycle", int)
    test_mode = RuntimeStateField("test_mode")
    auto_recording = RuntimeStateField("auto_recording", bool)
    cycle_recording_active = RuntimeStateField("cycle_recording_active", bool)

    is_stabilizing = RuntimeStateField("is_stabilizing", bool)
    stabilization_running = RuntimeStateField("stabilization_running", bool)
    stabilization_thread = RuntimeStateField("stabilization_thread")
    active_test_controller = RuntimeStateField("active_test_controller")
    active_test_power_source = RuntimeStateField("active_test_power_source")
    active_stabilization_controller = RuntimeStateField("active_stabilization_controller")
    active_stabilization_power_source = RuntimeStateField("active_stabilization_power_source")

    def __init__(self):
        super().__init__()
        self.runtime_state = AppRuntimeState()
        self.startup_service = StartupService(self)
        self.startup_service.bootstrap_main_window()

    def request_quit(self):
        return _require_registered_service(self, "lifecycle_service").request_quit()

    def _resolve_power_source_selection(self, selected_name: str) -> tuple[str, str]:
        return self.power_catalog_service.resolve_power_source_selection(selected_name)

    def apply_test_params(self, values: dict | None):
        updates = dict(values or {})
        if "power_source_name" in updates or "power_source" in updates:
            raw_source = updates.pop("power_source_name", updates.pop("power_source", AUTO_POWER_SOURCE_NAME))
            legacy_key, source_name = self._resolve_power_source_selection(raw_source)
            updates["power_source"] = legacy_key
            updates["power_source_name"] = source_name
        self.test_params.apply_update(updates)
        return self.test_params.as_dict()

    def apply_stabilization_params(self, values: dict | None):
        updates = dict(values or {})
        if "power_source_name" in updates or "power_source" in updates:
            raw_source = updates.pop("power_source_name", updates.pop("power_source", AUTO_POWER_SOURCE_NAME))
            legacy_key, source_name = self._resolve_power_source_selection(raw_source)
            updates["power_source"] = legacy_key
            updates["power_source_name"] = source_name
        self.stabilization_params.apply_update(updates)
        return self.stabilization_params.as_dict()

    def get_available_ports(self) -> list[str]:
        try:
            return self.panel_state_service.get_serial_port_list()
        except Exception:
            try:
                return [port.device for port in serial.tools.list_ports.comports()]
            except Exception:
                return []

    def get_record_file_path(self) -> str:
        return self.storage_service.get_record_file_path()

    def has_record_file_path(self) -> bool:
        return self.storage_service.has_record_file_path()

    def set_record_file_path(self, path: str):
        return self.storage_service.set_record_file_path(path)

    def get_record_output_dir(self) -> str:
        return self.storage_service.get_record_output_dir()

    def resolve_record_download_path(self, filename: str) -> str:
        return self.storage_service.resolve_record_download_path(filename)

    def normalize_power_type(self, power_type: str) -> str:
        return self.power_catalog_service.normalize_power_type(power_type)

    def _type_to_legacy_key(self, power_type: str) -> str:
        return self.power_catalog_service.type_to_legacy_key(power_type)

    def _power_source_name(self, source_key: str) -> str:
        return self.power_catalog_service.power_source_name(source_key)

    def _default_power_devices(self):
        return self.power_catalog_service.default_power_devices()

    def _ensure_unique_power_name(self, name: str, exclude_index: int | None = None) -> str:
        return self.power_catalog_service.ensure_unique_power_name(name, exclude_index=exclude_index)

    def _load_power_devices_from_config(self):
        return self.power_catalog_service.load_power_devices_from_config()

    def _power_devices_config_section(self):
        return self.power_catalog_service.power_devices_config_section()

    def _find_power_device_index(self, name: str):
        return self.power_catalog_service.find_power_device_index(name)

    def _find_power_device(self, name: str):
        return self.power_catalog_service.find_power_device(name)

    def list_power_source_names(self, include_auto: bool = True):
        return self.power_catalog_service.list_power_source_names(include_auto=include_auto)

    def _get_selected_power_name(self, module: str) -> str:
        return self.power_catalog_service.get_selected_power_name(module)

    def _connected_power_sources(self):
        return self.power_catalog_service.connected_power_sources()

    def _get_connected_keithley_names(self):
        return self.power_catalog_service.get_connected_keithley_names()

    def _refresh_keithley_controller_alias(self, preferred_name: str | None = None):
        return self.power_catalog_service.refresh_keithley_controller_alias(preferred_name)

    def _auto_bind_connected_power_to_modules(self, name: str):
        return self.power_catalog_service.auto_bind_connected_power_to_modules(name)

    def _power_device_type_matches(self, source_name: str, power_type: str) -> bool:
        return self.power_catalog_service.power_device_type_matches(source_name, power_type)

    def _resolve_power_controller(self, module: str, allow_auto: bool = True):
        return self.power_catalog_service.resolve_power_controller(module, allow_auto=allow_auto)

    def _can_execute_test_actions(self) -> bool:
        return self.power_catalog_service.can_execute_test_actions()

    def _can_execute_stabilization_actions(self) -> bool:
        return self.power_catalog_service.can_execute_stabilization_actions()

    def update_power_action_buttons(self):
        return self.power_panel_service.update_power_action_buttons()

    def _set_test_power_source(self, source_key: str):
        self.test_params['power_source'] = str(source_key or 'auto').strip().lower()

    def _set_stabilization_power_source(self, source_key: str):
        self.stabilization_params['power_source'] = str(source_key or 'auto').strip().lower()

    def _on_test_state_change(self, state: dict):
        """Sync UI buttons with test state changes (and handle countdown)."""
        self.test_runtime_service.handle_state_change(state)

    def _on_test_finished(self):
        """Test finished: stop auto-recording and compute single-test minima."""
        self.test_runtime_service.handle_finished()


    def setup_ui(self):
        return _require_registered_service(self, "window_ui_service").setup_ui()

    def setup_menu_bar(self):
        return _require_registered_service(self, "menu_service").setup_menu_bar()

    def show_meter_settings_dialog(self):
        return _require_registered_service(self, "dialog_service").show_meter_settings_dialog()

    def show_vacuum_settings_dialog(self):
        return _require_registered_service(self, "dialog_service").show_vacuum_settings_dialog()

    def show_power_settings_dialog(self):
        return _require_registered_service(self, "dialog_service").show_power_settings_dialog()

    def show_remote_influx_settings_dialog(self):
        return _require_registered_service(self, "dialog_service").show_remote_influx_settings_dialog()

    # -------- 曲线颜色（UI可配置）--------
    def _default_plot_colors(self):
        return self.plot_service.default_plot_colors()

    def get_plot_color(self, key, fallback='#000000'):
        return self.plot_service.get_plot_color(key, fallback=fallback)

    def _save_plot_colors_to_config(self, colors_dict):
        return self.plot_service.save_plot_colors_to_config(colors_dict)

    def apply_plot_colors(self):
        return self.plot_service.apply_plot_colors()

    def show_plot_color_settings(self):
        return self.plot_service.show_plot_color_settings()

    def show_plot_settings(self):
        return self.plot_service.show_plot_settings()

    def choose_single_plot_color(self, key: str, label: str = "曲线"):
        return self.plot_service.choose_single_plot_color(key, label=label)

    # -------- 菜单设置/动态绑定 --------
    def get_serial_port_list(self):
        return self.panel_state_service.get_serial_port_list()

    def get_meter_device_options(self):
        return self.panel_state_service.get_meter_device_options()

    def get_meter_device_option_dicts(self):
        return self.panel_state_service.get_meter_device_option_dicts()

    def get_meter_device_option(self, device_id: str):
        return self.panel_state_service.get_meter_device_option(device_id)

    def get_gpib_resource_list(self):
        return self.panel_state_service.get_gpib_resource_list()

    def get_meter_port(self, meter_type: str) -> str:
        return self.panel_state_service.get_meter_port(meter_type)

    def set_meter_port(self, meter_type: str, port_text: str):
        self.panel_state_service.set_meter_port(meter_type, port_text)

    def is_meter_connected(self, meter_type: str) -> bool:
        return self.panel_state_service.is_meter_connected(meter_type)

    def get_vacuum_type(self) -> str:
        return self.panel_state_service.get_vacuum_type()

    def set_vacuum_type(self, value: str):
        self.panel_state_service.set_vacuum_type(value)

    def get_vacuum_channel(self) -> str:
        return self.panel_state_service.get_vacuum_channel()

    def set_vacuum_channel(self, value: str):
        self.panel_state_service.set_vacuum_channel(value)

    def get_vacuum_baudrate(self) -> str:
        return self.panel_state_service.get_vacuum_baudrate()

    def set_vacuum_baudrate(self, value: str):
        self.panel_state_service.set_vacuum_baudrate(value)

    def get_vacuum_unit(self) -> str:
        return self.panel_state_service.get_vacuum_unit()

    def set_vacuum_unit(self, value: str):
        self.panel_state_service.set_vacuum_unit(value)

    def get_vacuum_alarm_max_pa(self) -> str:
        try:
            value = getattr(self, "vacuum_alarm_max_pa", None)
            if value is not None:
                return str(value)
        except Exception:
            pass
        try:
            return str(self.config.get("Safety", "vacuum_alarm_max_pa", fallback="1e-3"))
        except Exception:
            return "1e-3"

    def set_vacuum_alarm_max_pa(self, value):
        text = str(value or "").strip()
        self.vacuum_alarm_max_pa = text or "1e-3"

    def set_meter_connection(self, meter_type: str, should_connect: bool):
        return self.power_panel_service.set_meter_connection(meter_type, should_connect)

    def add_power_device(self):
        return self.power_panel_service.add_power_device()

    def remove_power_device(self, index: int):
        return self.power_panel_service.remove_power_device(index)

    def rename_power_device(self, index: int, new_name: str):
        self.device_manager.rename_power_device(index, new_name)

    def update_power_device_field(self, index: int, field: str, value: str):
        self.device_manager.update_power_device_field(index, field, value)

    def is_power_device_connected(self, name: str) -> bool:
        return self.device_manager.is_power_device_connected(name)

    def connect_named_power_device(self, name: str):
        return self.device_manager.connect_named_power_device(name)

    def disconnect_named_power_device(self, name: str):
        return self.device_manager.disconnect_named_power_device(name)

    def get_power_device_status_text(self, name: str) -> str:
        return self.device_manager.get_power_device_status_text(name)

    def update_power_summary_label(self):
        self.device_manager.update_power_summary_label()

    def _power_display_names(self) -> list[str]:
        return self.power_catalog_service.power_display_names()

    def _display_power_name(self, slot_index: int) -> str:
        return self.power_catalog_service.display_power_name(slot_index)

    def _set_power_voltage_cache(self, name: str, voltage: float):
        return self.display_service.set_power_voltage_cache(name, voltage)

    def _get_power_voltage_cache(self, name: str):
        return self.display_service.get_power_voltage_cache(name)

    def update_power_display_titles(self):
        return self.power_catalog_service.update_power_display_titles()

    def _set_voltage_label_state(self, label, text: str, state: str = 'disconnected'):
        return self.display_service.set_voltage_label_state(label, text, state)

    def _refresh_power_slot(self, slot_index: int, value_label):
        return self.display_service.refresh_power_slot(slot_index, value_label)

    def refresh_power_voltage_slots(self):
        return self.display_service.refresh_power_voltage_slots()

    def _get_record_power_name(self, module: str) -> str:
        return self.power_catalog_service.get_record_power_name(module)

    def _get_record_power_voltage(self, source_name: str):
        return self.power_catalog_service.get_record_power_voltage(source_name)

    def _build_record_headers(self):
        return self.power_catalog_service.build_record_headers()

    def register_service_runtime(self, *, web_start=None, web_stop=None, web_status=None, influx_start=None, influx_stop=None, influx_status=None):
        self._service_runtime = {
            'web_start': web_start,
            'web_stop': web_stop,
            'web_status': web_status,
            'influx_start': influx_start,
            'influx_stop': influx_stop,
            'influx_status': influx_status,
        }

    def get_remote_host(self) -> str:
        return self.panel_state_service.get_remote_host()

    def get_remote_port(self) -> int:
        return self.panel_state_service.get_remote_port()

    def set_remote_host(self, host: str):
        self.panel_state_service.set_remote_host(host)

    def set_remote_port(self, port):
        self.panel_state_service.set_remote_port(port)

    def is_remote_control_enabled(self) -> bool:
        return self.panel_state_service.is_remote_control_enabled()

    def set_remote_control_enabled(self, enabled: bool):
        self.panel_state_service.set_remote_control_enabled(enabled)

    def get_remote_status_text(self) -> str:
        return self.panel_state_service.get_remote_status_text()

    def _parse_influx_url(self):
        return self.panel_state_service.parse_influx_url()

    def get_influx_host(self) -> str:
        return self.panel_state_service.get_influx_host()

    def get_influx_port(self) -> str:
        return self.panel_state_service.get_influx_port()

    def set_influx_host_port(self, host: str, port):
        self.panel_state_service.set_influx_host_port(host, port)

    def get_influx_org(self) -> str:
        return self.panel_state_service.get_influx_org()

    def set_influx_org(self, value: str):
        self.panel_state_service.set_influx_org(value)

    def get_influx_bucket(self) -> str:
        return self.panel_state_service.get_influx_bucket()

    def set_influx_bucket(self, value: str):
        self.panel_state_service.set_influx_bucket(value)

    def get_influx_token(self) -> str:
        return self.panel_state_service.get_influx_token()

    def set_influx_token(self, value: str):
        self.panel_state_service.set_influx_token(value)

    def is_influx_enabled(self) -> bool:
        return self.panel_state_service.is_influx_enabled()

    def set_influx_enabled(self, enabled: bool):
        self.panel_state_service.set_influx_enabled(enabled)

    def get_influx_status_text(self) -> str:
        return self.panel_state_service.get_influx_status_text()

    def setup_controllers(self):
        """初始化控制器和数据"""
        self.bootstrap_service.setup_controllers()

    def setup_timers(self):
        self.timer_service.setup_timers()

    def refresh_gpib_ports(self):
        self.device_manager.refresh_gpib_ports()

    def _start_hv_connection_async(self, port: str, baudrate: int):
        self.device_manager.start_hv_connection_async(port, baudrate)

    def _on_hv_connect_finished(self, success: bool, message: str, port: str):
        self.device_manager.on_hv_connect_finished(success, message, port)



    def toggle_keithley_connection(self):
        self.device_manager.toggle_keithley_connection()

    def update_keithley_voltage(self):
        return self.display_service.update_keithley_voltage()

    def update_keithley_voltage_display(self, voltage, power_name: str | None = None):
        return self.display_service.update_keithley_voltage_display(voltage, power_name=power_name)

    def _current_source_combo_index(self, current_source: str) -> int:
        return self.settings_service.current_source_combo_index(current_source)

    def _combo_index_current_source(self, index: int) -> str:
        return self.settings_service.combo_index_current_source(index)

    def show_current_stabilization_settings(self):
        self.settings_service.show_current_stabilization_settings()

    def start_current_stabilization(self):
        """Public entry point used by GUI and web bridge."""
        self.stabilization_service.start()

    def _clear_stabilization_state(self):
        self.stabilization_service.clear_state()

    def _on_stabilization_thread_finished(self):
        self.stabilization_service.handle_thread_finished()

    def _start_current_stabilization_impl(self):
        self.stabilization_service.start_impl()

    def stop_current_stabilization(self):
        self.stabilization_service.stop_impl()

    def on_keithley_voltage_updated(self, voltage):
        """Legacy slot name retained for stabilization thread updates."""
        self.stabilization_service.on_voltage_updated(voltage)

    def on_stabilization_complete(self):
        """Notify when current enters the configured deadband."""
        self.stabilization_service.on_complete()

    def on_data_saved(self):
        self.lifecycle_service.on_data_saved()

    def on_data_converted(self):
        self.lifecycle_service.on_data_converted()

    def show_test_settings(self):
        self.settings_service.show_test_settings()

    def update_settings_display(self):
        """Refresh the compact settings summary shown in the control panel."""
        return self.settings_service.update_settings_display()

    def load_config_to_ui(self):
        """Load configuration values into UI widgets and parameter models."""
        self.config_sync_service.load_to_ui()

    def save_config_from_ui(self):
        """???????????????"""
        self.config_sync_service.save_from_ui()

    def refresh_all_ports(self):
        self.device_manager.refresh_all_ports()

    def refresh_ports(self):
        self.device_manager.refresh_ports()


    def toggle_hv_connection(self):
        self.device_manager.toggle_hv_connection()

    def manual_set_voltage(self):
        return self.manual_control_service.manual_set_voltage()

    def toggle_meter_connection(self, meter_type):
        self.device_manager.toggle_meter_connection(meter_type)

    def handle_meter_data(self, data):
        return self.meter_data_service.handle_meter_data(data)

    def update_meter_displays(self):
        return self.display_service.update_meter_displays()

    def _attach_hv_worker_signals(self):
        self.device_manager.attach_hv_worker_signals()

    def _detach_hv_worker_signals(self):
        self.device_manager.detach_hv_worker_signals()

    def _on_hv_worker_error(self, msg: str):
        self.device_manager.on_hv_worker_error(msg)

    def _on_hv_worker_connected(self, port: str):
        self.device_manager.on_hv_worker_connected(port)

    def _on_hv_worker_disconnected(self):
        self.device_manager.on_hv_worker_disconnected()


    def start_hv_voltage_poller(self, interval_ms: int = 500):
        self.device_manager.start_hv_voltage_poller(interval_ms=interval_ms)

    def stop_hv_voltage_poller(self):
        self.device_manager.stop_hv_voltage_poller()

    def on_hv_voltage_polled(self, voltage: float):
        self.device_manager.on_hv_voltage_polled(voltage)

    def _on_hv_poller_error(self, msg: str):
        self.device_manager.on_hv_poller_error(msg)

    def _get_display_keithley_controller(self):
        return self.power_panel_service.get_display_keithley_controller()

    def _get_connected_keithley_controller_map(self):
        return self.device_manager.get_connected_keithley_controller_map()

    def start_keithley_voltage_poller(self, interval_ms: int = 2500):
        self.device_manager.start_keithley_voltage_poller(interval_ms=interval_ms)

    def stop_keithley_voltage_poller(self):
        self.device_manager.stop_keithley_voltage_poller()

    def _on_keithley_poller_voltage(self, voltage: float):
        self.device_manager.on_keithley_poller_voltage(voltage)

    def _on_named_keithley_poller_voltage(self, name: str, voltage: float):
        self.device_manager.on_named_keithley_poller_voltage(name, voltage)

    def _on_keithley_poller_error(self, msg: str):
        self.device_manager.on_keithley_poller_error(msg)


    def update_hv_voltage(self):
        return self.display_service.update_hv_voltage()

    def update_hv_voltage_display(self, voltage, power_name: str | None = None):
        return self.display_service.update_hv_voltage_display(voltage, power_name=power_name)

    def update_status_display(self):
        return self.display_service.update_status_display()

    def update_countdown_display(self, countdown):
        return self.display_service.update_countdown_display(countdown)

    def update_plots(self):
        return self.plot_service.update_plots()

    def start_test(self):
        """开始单次测试"""
        self.test_service.start(cycle=False)

    def start_cycle_test(self):
        """开始循环测试"""
        self.test_service.start(cycle=True)

    def _start_test(self, cycle=False):
        """Compatibility wrapper: delegate legacy callers to TestService."""
        return self.test_service.start(cycle=cycle)

    def run_test(self, start_voltage, target_voltage, voltage_step, step_delay, cycle_time, is_cycle):
        """Compatibility wrapper: delegate legacy callers to TestService."""
        return self.test_control_service.run_test(
            start_voltage,
            target_voltage,
            voltage_step,
            step_delay,
            cycle_time,
            is_cycle,
        )

    def calculate_and_save_cycle_min(self):
        """计算并保存当前循环的最小阳极值和对应电压及时间"""
        return self.test_control_service.calculate_and_save_cycle_min()

    def _update_ui_after_test(self):
        """测试结束后更新UI"""
        return self.test_control_service.update_ui_after_test()

    def stop_test(self):
        """停止测试"""
        return self.test_control_service.stop_test()

    def reset_voltage(self):
        """复位到100V"""
        return self.test_control_service.reset_voltage()

    def emergency_stop(self):
        return self.test_control_service.emergency_stop()

    def toggle_record(self):
        """??????????"""
        return self.recording_service.toggle_record()

    def save_data(self):
        """?????? - ??????"""
        self.recording_service.save_data()

    def flush_data_cache(self, force: bool = False):
        """??????????????????????"""
        self.recording_service.flush_data_cache(force=force)

    def calculate_and_save_anode_min(self):
        """??????????????????????????????"""
        self.recording_service.calculate_and_save_anode_min()

    def save_recorded_data(self):
        """??????????????? cycle.csv / summary.csv????????"""
        self.recording_service.save_recorded_data()

    def select_path(self):
        """?????????"""
        self.recording_service.select_path()

    def get_sqlite_db_path(self) -> str:
        return self.storage_service.get_sqlite_db_path()

    def get_db_stats(self) -> dict:
        """Return SQLite database stats for GUI/Web diagnostics."""
        return self.maintenance_service.get_db_stats()

    def cleanup_database(self, *, keep_days: int, keep_runs: int, archive_before_delete: bool, archive_dir: str, vacuum_mode: str):
        return self.maintenance_service.cleanup_database(
            keep_days=keep_days,
            keep_runs=keep_runs,
            archive_before_delete=archive_before_delete,
            archive_dir=archive_dir,
            vacuum_mode=vacuum_mode,
        )

    def on_db_cleanup_clicked(self):
        """GUI handler: one-click cleanup."""
        return self.maintenance_service.on_db_cleanup_clicked()

    def update_db_status_label(self):
        return self.maintenance_service.update_db_status_label()

    def clear_plots(self):
        return self.plot_service.clear_plots()

    def show_status_message(self, message, timeout_ms: int = 0):
        service = getattr(self, "feedback_service", None)
        if service is not None:
            return service.show_status_message(message, timeout_ms=timeout_ms)
        try:
            if int(timeout_ms or 0) > 0:
                self.status_bar.showMessage(str(message), int(timeout_ms))
            else:
                self.status_bar.showMessage(str(message))
        except Exception:
            pass
        return str(message)

    def log_message(self, message):
        return _require_registered_service(self, "feedback_service").log_message(message)

    def closeEvent(self, event):
        return _require_registered_service(self, "lifecycle_service").close_event(event)
