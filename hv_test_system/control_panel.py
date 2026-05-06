from __future__ import annotations

from .common import *


class ControlPanel(QWidget):
    """Main control-side panel for device, record, and system actions."""

    TITLE = "\u9ad8\u538b\u7535\u6e90\u4e0e\u4e07\u7528\u8868\u6d4b\u8bd5\u7cfb\u7edf"
    OPERATION_GROUP_TITLE = "\u6d4b\u8bd5\u4e0e\u7a33\u6d41\u63a7\u5236"
    METER_GROUP_TITLE = "\u4e07\u7528\u8868\u7cfb\u6570"
    RECORD_GROUP_TITLE = "\u6570\u636e\u8bb0\u5f55"
    CONTROL_GROUP_TITLE = "\u7cfb\u7edf\u63a7\u5236"
    LOG_GROUP_TITLE = "\u7cfb\u7edf\u6d88\u606f"
    UNCONNECTED_TEXT = "\u672a\u8fde\u63a5"
    METER_GROUP_HINT = "\u66f2\u7ebf\u989c\u8272\u548c\u56fe\u8868\u70b9\u4f4d\u5728\u83dc\u5355\u680f\u201c\u56fe\u7ebf\u8bbe\u7f6e\u201d\u4e2d\u7edf\u4e00\u8c03\u6574\u3002"
    METER_CONFIGS = (
        ("\u9634\u6781", "cathode"),
        ("\u6805\u6781", "gate"),
        ("\u9633\u6781", "anode"),
        ("\u6536\u96c6\u6781", "backup"),
        ("\u771f\u7a7a", "vacuum"),
    )

    def __init__(self, main_window):
        super().__init__()
        self.main_window = main_window
        self.setup_ui()

    def _compat_host(self):
        host = getattr(self.main_window, "_compat_widget_host", None)
        if host is None:
            host = QWidget(self)
            host.setObjectName("_compat_widget_host")
            host.setFixedSize(0, 0)
            host.move(-10000, -10000)
            host.hide()
            self.main_window._compat_widget_host = host
        return host

    def setup_ui(self):
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(8, 8, 8, 8)
        main_layout.setSpacing(12)
        main_layout.addWidget(self._create_title_label())
        self._make_hidden_compat_widgets()
        main_layout.addWidget(self._create_scroll_area())

    def _create_scroll_area(self):
        self.tabs = QTabWidget()
        self._add_control_panel_sections(self.tabs)
        return self.tabs

    def _add_control_panel_sections(self, tabs):
        tabs.addTab(self._create_hardware_tab(), "硬件监控")
        tabs.addTab(self._create_test_tab(), "测试预设")
        tabs.addTab(self._create_system_tab(), "系统维护")

    def _create_hardware_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(8, 12, 8, 8)
        layout.setSpacing(10)
        layout.addWidget(self._create_power_group())
        layout.addWidget(self.create_meter_group())
        layout.addStretch()
        return tab

    def _create_power_group(self):
        power_box = QGroupBox("电源监控与手动控制")
        power_layout = QGridLayout(power_box)
        power_layout.setVerticalSpacing(8)
        power_layout.setHorizontalSpacing(8)
        power_layout.setContentsMargins(12, 16, 12, 12)
        self._add_power_summary_row(power_layout)
        self._add_voltage_status_row(power_layout)
        self._add_manual_voltage_row(power_layout)
        return power_box

    def _create_test_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(8, 12, 8, 8)
        layout.setSpacing(10)
        layout.addWidget(self.create_operation_group(), 1)
        return tab

    def _create_system_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(8, 12, 8, 8)
        layout.setSpacing(10)
        layout.addWidget(self.create_record_group())
        layout.addWidget(self.create_control_group())
        layout.addWidget(self.create_log_group())
        return tab

    def create_operation_group(self):
        action_box = QGroupBox("自动执行与参数设置")
        action_layout = QVBoxLayout(action_box)
        action_layout.setSpacing(12)
        action_layout.setContentsMargins(12, 18, 12, 12)
        self._add_current_settings_row(action_layout)
        self._add_operation_action_row(action_layout)
        self._add_stabilization_action_row(action_layout)
        self._add_emergency_stop_row(action_layout)
        return action_box

    def _create_title_label(self):
        title_label = QLabel(self.TITLE)
        title_label.setObjectName("titleLabel")
        title_label.setAlignment(Qt.AlignCenter)
        title_label.setMinimumHeight(40)
        return title_label

    def _make_hidden_compat_widgets(self):
        host = self._compat_host()
        self._create_hidden_hv_widgets(host)
        self._create_hidden_keithley_widgets(host)

    def _create_hidden_hv_widgets(self, host):
        self.main_window.hv_port_combo = QComboBox(host)
        self.main_window.hv_port_combo.hide()

        self.main_window.hv_baudrate_combo = QComboBox(host)
        self.main_window.hv_baudrate_combo.addItems(["9600", "19200", "38400", "57600"])
        self.main_window.hv_baudrate_combo.setCurrentText("9600")
        self.main_window.hv_baudrate_combo.hide()

        self.main_window.hv_refresh_btn = QPushButton("\u5237\u65b0", host)
        self.main_window.hv_refresh_btn.hide()

        self.main_window.hv_connect_btn = QPushButton("\u8fde\u63a5\u9ad8\u538b\u6e90", host)
        self.main_window.hv_connect_btn.hide()

    def _create_hidden_keithley_widgets(self, host):
        self.main_window.keithley_addr_combo = QComboBox(host)
        self.main_window.keithley_addr_combo.setEditable(True)
        self.main_window.keithley_addr_combo.hide()

        self.main_window.keithley_connect_btn = QPushButton("\u8fde\u63a5", host)
        self.main_window.keithley_connect_btn.hide()

    def _add_power_summary_row(self, layout):
        layout.addWidget(QLabel("\u5df2\u8fde\u63a5\u7535\u6e90"), 0, 0)
        self.main_window.power_summary_label = QLabel(self.UNCONNECTED_TEXT)
        self.main_window.power_summary_label.setObjectName("settingsLabel")
        self.main_window.power_summary_label.setMinimumHeight(28)
        self.main_window.power_summary_label.setWordWrap(True)
        layout.addWidget(self.main_window.power_summary_label, 0, 1, 1, 3)

    def _add_voltage_status_row(self, layout):
        self.main_window.hv_voltage_title_label = QLabel("\u7535\u6e901\u7535\u538b:")
        layout.addWidget(self.main_window.hv_voltage_title_label, 1, 0)
        self.main_window.hv_voltage_label = QLabel(self.UNCONNECTED_TEXT)
        self.main_window.hv_voltage_label.setObjectName("voltageLabel")
        self.main_window.hv_voltage_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(self.main_window.hv_voltage_label, 1, 1)

        self.main_window.keithley_voltage_title_label = QLabel("\u7535\u6e902\u7535\u538b:")
        layout.addWidget(self.main_window.keithley_voltage_title_label, 1, 2)
        self.main_window.keithley_voltage_label = QLabel(self.UNCONNECTED_TEXT)
        self.main_window.keithley_voltage_label.setObjectName("voltageLabel")
        self.main_window.keithley_voltage_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(self.main_window.keithley_voltage_label, 1, 3)

    def _add_manual_voltage_row(self, layout):
        layout.addWidget(QLabel("手动设置电压(V):"), 2, 0)
        
        self.main_window.manual_voltage_target_combo = QComboBox()
        self.main_window.manual_voltage_target_combo.setMinimumHeight(28)
        self.main_window.manual_voltage_target_combo.setMinimumWidth(80)
        layout.addWidget(self.main_window.manual_voltage_target_combo, 2, 1)

        self.main_window.manual_voltage_edit = QLineEdit("0")
        self.main_window.manual_voltage_edit.setValidator(QtGui.QDoubleValidator(0, 10000, 1))
        self.main_window.manual_voltage_edit.setMinimumHeight(28)
        self.main_window.manual_voltage_edit.setPlaceholderText(
            "输入电压值并回车设置"
        )
        self.main_window.manual_voltage_edit.returnPressed.connect(self.main_window.manual_set_voltage)
        layout.addWidget(self.main_window.manual_voltage_edit, 2, 2)

        self.main_window.manual_set_btn = QPushButton("设置")
        self.main_window.manual_set_btn.setMinimumHeight(28)
        self.main_window.manual_set_btn.clicked.connect(self.main_window.manual_set_voltage)
        self.main_window.manual_set_btn.setEnabled(False)
        layout.addWidget(self.main_window.manual_set_btn, 2, 3)

        self.main_window.countdown_label = QLabel("")
        self.main_window.countdown_label.setObjectName("countdown")
        self.main_window.countdown_label.hide()
        layout.addWidget(self.main_window.countdown_label, 3, 0, 1, 4)

    def _add_current_settings_row(self, layout):
        self.main_window.current_settings_label = QLabel(
            "\u5f53\u524d\u8bbe\u7f6e: \u672a\u914d\u7f6e"
        )
        self.main_window.current_settings_label.setWordWrap(True)
        self.main_window.current_settings_label.setObjectName("settingsLabel")
        self.main_window.current_settings_label.setMinimumHeight(64)
        layout.addWidget(self.main_window.current_settings_label)

    def _create_operation_section(self, title):
        section = QGroupBox(title)
        section.setMinimumHeight(118)
        section.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Expanding)

        section_layout = QGridLayout(section)
        section_layout.setVerticalSpacing(10)
        section_layout.setHorizontalSpacing(8)
        section_layout.setContentsMargins(10, 18, 10, 10)
        for column in range(4):
            section_layout.setColumnStretch(column, 1)
        return section, section_layout

    def _add_operation_settings_row(self, layout):
        self._add_operation_action_row(layout)

    def _add_operation_action_row(self, layout):
        section, section_layout = self._create_operation_section("\u5347\u538b\u6d4b\u8bd5")

        self.main_window.settings_btn = QPushButton("\u5347\u538b\u6d4b\u8bd5\u8bbe\u7f6e")
        self.main_window.settings_btn.setMinimumHeight(32)
        self.main_window.settings_btn.clicked.connect(self.main_window.show_test_settings)
        section_layout.addWidget(self.main_window.settings_btn, 0, 0, 1, 4)

        self.main_window.start_test_btn = QPushButton("\u5f00\u59cb\u6d4b\u8bd5")
        self.main_window.start_test_btn.setMinimumHeight(32)
        self.main_window.start_test_btn.clicked.connect(self.main_window.start_test)
        self.main_window.start_test_btn.setEnabled(False)
        section_layout.addWidget(self.main_window.start_test_btn, 1, 0)

        self.main_window.cycle_test_btn = QPushButton("\u5faa\u73af\u6d4b\u8bd5")
        self.main_window.cycle_test_btn.setMinimumHeight(32)
        self.main_window.cycle_test_btn.clicked.connect(self.main_window.start_cycle_test)
        self.main_window.cycle_test_btn.setEnabled(False)
        section_layout.addWidget(self.main_window.cycle_test_btn, 1, 1)

        self.main_window.stop_test_btn = QPushButton("\u505c\u6b62\u6d4b\u8bd5")
        self.main_window.stop_test_btn.setMinimumHeight(32)
        self.main_window.stop_test_btn.clicked.connect(self.main_window.stop_test)
        self.main_window.stop_test_btn.setEnabled(False)
        section_layout.addWidget(self.main_window.stop_test_btn, 1, 2, 1, 2)

        layout.addWidget(section, 1)

    def _add_stabilization_action_row(self, layout):
        section, section_layout = self._create_operation_section("\u7a33\u6d41\u63a7\u5236")

        self.main_window.current_stabilization_btn = QPushButton("\u7a33\u6d41\u8bbe\u7f6e")
        self.main_window.current_stabilization_btn.setMinimumHeight(32)
        self.main_window.current_stabilization_btn.clicked.connect(
            self.main_window.show_current_stabilization_settings
        )
        self.main_window.current_stabilization_btn.setEnabled(False)
        section_layout.addWidget(self.main_window.current_stabilization_btn, 0, 0, 1, 4)

        self.main_window.start_stabilization_btn = QPushButton("\u5f00\u59cb\u7a33\u6d41")
        self.main_window.start_stabilization_btn.setMinimumHeight(32)
        self.main_window.start_stabilization_btn.clicked.connect(
            self.main_window.start_current_stabilization
        )
        self.main_window.start_stabilization_btn.setEnabled(False)
        section_layout.addWidget(self.main_window.start_stabilization_btn, 1, 0, 1, 2)

        self.main_window.stop_stabilization_btn = QPushButton("\u505c\u6b62\u7a33\u6d41")
        self.main_window.stop_stabilization_btn.setMinimumHeight(32)
        self.main_window.stop_stabilization_btn.clicked.connect(
            self.main_window.stop_current_stabilization
        )
        self.main_window.stop_stabilization_btn.setEnabled(False)
        section_layout.addWidget(self.main_window.stop_stabilization_btn, 1, 2, 1, 2)

        layout.addWidget(section, 1)

    def _add_emergency_stop_row(self, layout):
        self.main_window.reset_btn = QPushButton("紧急停止")
        self.main_window.reset_btn.setMinimumHeight(34)
        self.main_window.reset_btn.clicked.connect(self.main_window.emergency_stop)
        self.main_window.reset_btn.setEnabled(False)
        layout.addWidget(self.main_window.reset_btn)
        self.main_window.emergency_stop_shortcut = QShortcut(QtGui.QKeySequence("Escape"), self.main_window)
        self.main_window.emergency_stop_shortcut.activated.connect(self.main_window.emergency_stop)
        self.main_window.emergency_stop_shortcut_f12 = QShortcut(QtGui.QKeySequence("F12"), self.main_window)
        self.main_window.emergency_stop_shortcut_f12.activated.connect(self.main_window.emergency_stop)

    def _add_operation_stop_row(self, layout):
        self._add_emergency_stop_row(layout)

    def create_meter_group(self):
        group = QGroupBox(self.METER_GROUP_TITLE)
        layout = self._create_meter_group_layout(group)
        self._add_meter_group_hint(layout)
        self._add_meter_group_headers(layout)

        for row, (label, meter_type) in enumerate(self.METER_CONFIGS, start=2):
            self._add_meter_group_row(layout, row, label, meter_type)

        self._finalize_meter_group_layout(layout)
        return group

    def _create_meter_group_layout(self, group):
        layout = QGridLayout(group)
        layout.setVerticalSpacing(6)
        layout.setHorizontalSpacing(8)
        layout.setContentsMargins(8, 15, 8, 8)
        return layout

    def _add_meter_group_hint(self, layout):
        hint = QLabel(self.METER_GROUP_HINT)
        hint.setWordWrap(True)
        hint.setObjectName("settingsLabel")
        hint.setMinimumHeight(28)
        layout.addWidget(hint, 0, 0, 1, 3)

    def _add_meter_group_headers(self, layout):
        layout.addWidget(QLabel("\u901a\u9053"), 1, 0)
        layout.addWidget(QLabel("\u7cfb\u6570"), 1, 1)
        layout.addWidget(QLabel("\u5f53\u524d\u503c"), 1, 2)

    def _add_meter_group_row(self, layout, row, label, meter_type):
        layout.addWidget(QLabel(label), row, 0)
        self._create_hidden_meter_port_widgets(meter_type)

        coeff_edit = QLineEdit("1.0")
        coeff_edit.setMinimumHeight(28)
        coeff_edit.setMaximumWidth(90)
        setattr(self.main_window, f"{meter_type}_coeff", coeff_edit)
        layout.addWidget(coeff_edit, row, 1)

        value_label = QLabel(self.UNCONNECTED_TEXT)
        value_label.setObjectName("meterValue")
        value_label.setAlignment(Qt.AlignCenter)
        value_label.setMinimumHeight(28)
        setattr(self.main_window, f"{meter_type}_value_label", value_label)
        layout.addWidget(value_label, row, 2)

    def _create_hidden_meter_port_widgets(self, meter_type):
        compat_host = self._compat_host()

        port_combo = QComboBox(compat_host)
        port_combo.setEditable(True)
        port_combo.hide()
        setattr(self.main_window, f"{meter_type}_port_combo", port_combo)

        connect_btn = QPushButton("\u8fde\u63a5", compat_host)
        connect_btn.hide()
        setattr(self.main_window, f"{meter_type}_connect_btn", connect_btn)

    def _finalize_meter_group_layout(self, layout):
        layout.setColumnStretch(0, 0)
        layout.setColumnStretch(1, 0)
        layout.setColumnStretch(2, 1)

    def create_record_group(self):
        group = QGroupBox(self.RECORD_GROUP_TITLE)
        layout = self._create_record_group_layout(group)
        self._add_record_path_row(layout)
        self._add_record_interval_row(layout)
        self._add_record_button(layout)
        self._add_database_status_row(layout)
        self._add_database_maintenance_row(layout)
        self._add_database_archive_row(layout)
        self._refresh_database_status_label()
        return group

    def _create_record_group_layout(self, group):
        layout = QVBoxLayout(group)
        layout.setSpacing(6)
        layout.setContentsMargins(8, 15, 8, 8)
        return layout

    def _add_record_path_row(self, layout):
        path_layout = QHBoxLayout()
        self.main_window.path_btn = QPushButton("\u9009\u62e9\u8def\u5f84")
        self.main_window.path_btn.setMinimumHeight(28)
        self.main_window.path_btn.clicked.connect(self.main_window.select_path)
        path_layout.addWidget(self.main_window.path_btn)

        self.main_window.path_label = QLabel("\u672a\u9009\u62e9\u4fdd\u5b58\u8def\u5f84")
        self.main_window.path_label.setWordWrap(True)
        self.main_window.path_label.setObjectName("pathLabel")
        self.main_window.path_label.setMinimumHeight(40)
        path_layout.addWidget(self.main_window.path_label)
        layout.addLayout(path_layout)

    def _add_record_interval_row(self, layout):
        interval_layout = QHBoxLayout()
        interval_layout.addWidget(QLabel("\u4fdd\u5b58\u95f4\u9694(s):"))
        self.main_window.interval_edit = QLineEdit("1")
        self.main_window.interval_edit.setMaximumWidth(50)
        self.main_window.interval_edit.setMinimumHeight(26)
        self.main_window.interval_edit.setValidator(QtGui.QIntValidator(1, 3600))
        interval_layout.addWidget(self.main_window.interval_edit)
        interval_layout.addStretch()
        layout.addLayout(interval_layout)

    def _add_record_button(self, layout):
        self.main_window.record_btn = QPushButton("\u5f00\u59cb\u8bb0\u5f55")
        self.main_window.record_btn.setMinimumHeight(30)
        self.main_window.record_btn.clicked.connect(self.main_window.toggle_record)
        self.main_window.record_btn.setEnabled(True)
        layout.addWidget(self.main_window.record_btn)

    def _add_database_status_row(self, layout):
        status_row = QHBoxLayout()
        self.main_window.db_status_label = QLabel("SQLite: -")
        self.main_window.db_status_label.setWordWrap(True)
        self.main_window.db_status_label.setMinimumHeight(22)
        status_row.addWidget(self.main_window.db_status_label)
        layout.addLayout(status_row)

    def _add_database_maintenance_row(self, layout):
        maint = QHBoxLayout()
        maint.addWidget(QLabel("\u4fdd\u7559\u5929\u6570:"))
        self.main_window.db_keep_days_edit = QLineEdit("30")
        self.main_window.db_keep_days_edit.setMaximumWidth(60)
        self.main_window.db_keep_days_edit.setValidator(QtGui.QIntValidator(1, 36500))
        maint.addWidget(self.main_window.db_keep_days_edit)
        maint.addWidget(QLabel("\u4fdd\u7559 runs:"))
        self.main_window.db_keep_runs_edit = QLineEdit("200")
        self.main_window.db_keep_runs_edit.setMaximumWidth(70)
        self.main_window.db_keep_runs_edit.setValidator(QtGui.QIntValidator(1, 1000000))
        maint.addWidget(self.main_window.db_keep_runs_edit)
        self.main_window.db_archive_chk = QCheckBox("\u6e05\u7406\u524d\u5f52\u6863 (CSV)")
        self.main_window.db_archive_chk.setChecked(True)
        maint.addWidget(self.main_window.db_archive_chk)
        maint.addWidget(QLabel("Vacuum:"))
        self.main_window.db_vacuum_mode_combo = QComboBox()
        self.main_window.db_vacuum_mode_combo.addItem("\u589e\u91cf", "incremental")
        self.main_window.db_vacuum_mode_combo.addItem("\u5168\u91cf", "vacuum")
        self.main_window.db_vacuum_mode_combo.setMaximumWidth(70)
        maint.addWidget(self.main_window.db_vacuum_mode_combo)
        maint.addStretch()
        layout.addLayout(maint)

    def _add_database_archive_row(self, layout):
        arch_row = QHBoxLayout()
        arch_row.addWidget(QLabel("\u5f52\u6863\u76ee\u5f55:"))
        self.main_window.db_archive_dir_edit = QLineEdit(os.path.join("data", "archive"))
        self.main_window.db_archive_dir_edit.setMinimumHeight(26)
        arch_row.addWidget(self.main_window.db_archive_dir_edit)
        self.main_window.db_cleanup_btn = QPushButton("\u4e00\u952e\u6e05\u7406\u6570\u636e\u5e93")
        self.main_window.db_cleanup_btn.setMinimumHeight(28)
        self.main_window.db_cleanup_btn.clicked.connect(self.main_window.on_db_cleanup_clicked)
        arch_row.addWidget(self.main_window.db_cleanup_btn)
        layout.addLayout(arch_row)

    def _refresh_database_status_label(self):
        try:
            self.main_window.update_db_status_label()
        except Exception:
            pass

    def create_control_group(self):
        group = QGroupBox(self.CONTROL_GROUP_TITLE)
        layout = QHBoxLayout(group)
        layout.setContentsMargins(8, 15, 8, 8)

        self.main_window.clear_btn = QPushButton("\u6e05\u7a7a\u56fe\u8868")
        self.main_window.clear_btn.setMinimumHeight(30)
        self.main_window.clear_btn.clicked.connect(self.main_window.clear_plots)
        layout.addWidget(self.main_window.clear_btn)

        self.main_window.refresh_ports_btn = QPushButton("\u5237\u65b0\u7aef\u53e3")
        self.main_window.refresh_ports_btn.setMinimumHeight(30)
        self.main_window.refresh_ports_btn.clicked.connect(self.main_window.refresh_all_ports)
        layout.addWidget(self.main_window.refresh_ports_btn)
        layout.addStretch()
        return group

    def create_log_group(self):
        group = QGroupBox(self.LOG_GROUP_TITLE)
        layout = QVBoxLayout(group)
        layout.setContentsMargins(8, 15, 8, 8)
        self.main_window.log_text = QPlainTextEdit()
        self.main_window.log_text.setReadOnly(True)
        self.main_window.log_text.setMinimumHeight(120)
        try:
            self.main_window.log_text.setMaximumBlockCount(1000)
        except Exception:
            pass
        layout.addWidget(self.main_window.log_text)
        return group
