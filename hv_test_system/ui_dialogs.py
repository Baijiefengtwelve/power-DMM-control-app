from __future__ import annotations

from PyQt5 import QtCore
from PyQt5 import QtGui
from PyQt5.QtCore import QSize, Qt
from PyQt5.QtGui import QColor
from PyQt5.QtWidgets import (
    QCheckBox,
    QColorDialog,
    QComboBox,
    QDialog,
    QFormLayout,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QVBoxLayout,
    QWidget,
)
from .settings_dialog_models import (
    StabilizationSettingsDialogState,
    TestSettingsDialogState,
)
class ToggleSwitch(QCheckBox):
    """iPhone-style animated switch used across settings dialogs."""
    def __init__(self, parent=None, on_text="已连接", off_text="未连接"):
        super().__init__(parent)
        self.on_text = on_text
        self.off_text = off_text
        self._track_width = 52
        self._track_height = 32
        self._margin = 2
        self._offset = 1.0 if self.isChecked() else 0.0
        self._animation = QtCore.QPropertyAnimation(self, b"offset", self)
        self._animation.setDuration(180)
        self._animation.setEasingCurve(QtCore.QEasingCurve.OutCubic)
        self.setCursor(Qt.PointingHandCursor)
        self.setFocusPolicy(Qt.StrongFocus)
        self.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.setFixedSize(self.sizeHint())
        self.setText("")
        self.setToolTip(self.off_text)
        self.toggled.connect(self._animate)
        self._update_accessibility(self.isChecked())
    def sizeHint(self):
        return QSize(self._track_width + 8, self._track_height + 8)
    @QtCore.pyqtProperty(float)
    def offset(self):
        return self._offset
    @offset.setter
    def offset(self, value):
        self._offset = max(0.0, min(1.0, float(value)))
        self.update()
    def _animate(self, checked: bool):
        self._animation.stop()
        self._animation.setStartValue(self._offset)
        self._animation.setEndValue(1.0 if checked else 0.0)
        self._animation.start()
        self._update_accessibility(checked)
    def _update_accessibility(self, checked: bool):
        state_text = self.on_text if checked else self.off_text
        self.setToolTip(state_text)
        self.setAccessibleName(state_text)
        self.setAccessibleDescription(state_text)
    def hitButton(self, pos):
        return self.contentsRect().contains(pos)
    def mouseReleaseEvent(self, event):
        if event.button() == Qt.LeftButton and self.rect().contains(event.pos()):
            super().mouseReleaseEvent(event)
            return
        event.ignore()
    def paintEvent(self, event):
        del event
        painter = QtGui.QPainter(self)
        painter.setRenderHint(QtGui.QPainter.Antialiasing)
        outer_rect = self.rect().adjusted(4, 4, -4, -4)
        track_rect = QtCore.QRectF(outer_rect)
        radius = track_rect.height() / 2.0
        if self.isEnabled():
            if self.isChecked():
                track_color = QtGui.QColor("#34C759")
                border_color = QtGui.QColor("#30B852")
            else:
                track_color = QtGui.QColor("#E5E5EA")
                border_color = QtGui.QColor("#D1D1D6")
            knob_color = QtGui.QColor("#FFFFFF")
        else:
            if self.isChecked():
                track_color = QtGui.QColor("#A8DCA8")
                border_color = QtGui.QColor("#9FD09F")
            else:
                track_color = QtGui.QColor("#EFEFF4")
                border_color = QtGui.QColor("#DDDEE3")
            knob_color = QtGui.QColor("#FAFAFB")
        painter.setPen(QtGui.QPen(border_color, 1))
        painter.setBrush(track_color)
        painter.drawRoundedRect(track_rect, radius, radius)
        knob_diameter = track_rect.height() - self._margin * 2
        travel = track_rect.width() - knob_diameter - self._margin * 2
        knob_x = track_rect.x() + self._margin + (travel * self._offset)
        knob_y = track_rect.y() + self._margin
        knob_rect = QtCore.QRectF(knob_x, knob_y, knob_diameter, knob_diameter)
        shadow_rect = knob_rect.adjusted(0.5, 1.2, 0.5, 1.8)
        painter.setPen(Qt.NoPen)
        painter.setBrush(QtGui.QColor(0, 0, 0, 28 if self.isEnabled() else 14))
        painter.drawEllipse(shadow_rect)
        gradient = QtGui.QRadialGradient(knob_rect.center(), knob_rect.width() * 0.75)
        gradient.setColorAt(0.0, QtGui.QColor("#FFFFFF"))
        gradient.setColorAt(0.78, knob_color)
        gradient.setColorAt(1.0, QtGui.QColor("#F1F1F4"))
        painter.setBrush(QtGui.QBrush(gradient))
        painter.setPen(QtGui.QPen(QtGui.QColor(0, 0, 0, 18), 1))
        painter.drawEllipse(knob_rect)
        if self.hasFocus():
            focus_rect = track_rect.adjusted(-2, -2, 2, 2)
            focus_pen = QtGui.QPen(QtGui.QColor("#0A84FF"), 2)
            focus_pen.setCosmetic(True)
            painter.setPen(focus_pen)
            painter.setBrush(Qt.NoBrush)
            painter.drawRoundedRect(focus_rect, focus_rect.height() / 2.0, focus_rect.height() / 2.0)

# Legacy dialog implementations kept temporarily for reference; active dialog
# classes are defined later in this module.
class _LegacyTestSettingsDialog(QDialog):
    """测试参数设置对话框"""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("测试参数设置")
        self.setModal(True)
        self.setup_ui()
    def setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(10)
        layout.setContentsMargins(15, 15, 15, 15)
        form_layout = QFormLayout()
        form_layout.setVerticalSpacing(8)
        form_layout.setHorizontalSpacing(10)
        self.start_voltage_edit = QLineEdit("0")
        self.start_voltage_edit.setValidator(QtGui.QDoubleValidator(0, 10000, 1))
        self.start_voltage_edit.setMinimumHeight(25)
        form_layout.addRow("起始电压 (V):", self.start_voltage_edit)
        self.target_voltage_edit = QLineEdit("1000")
        self.target_voltage_edit.setValidator(QtGui.QDoubleValidator(0, 10000, 1))
        self.target_voltage_edit.setMinimumHeight(25)
        form_layout.addRow("目标电压 (V):", self.target_voltage_edit)
        self.voltage_step_edit = QLineEdit("10")
        self.voltage_step_edit.setValidator(QtGui.QDoubleValidator(0.1, 1000, 1))
        self.voltage_step_edit.setMinimumHeight(25)
        form_layout.addRow("电压增幅 (V):", self.voltage_step_edit)
        self.step_delay_edit = QLineEdit("1")
        self.step_delay_edit.setValidator(QtGui.QDoubleValidator(0.1, 60, 1))
        self.step_delay_edit.setMinimumHeight(25)
        form_layout.addRow("升压延迟 (s):", self.step_delay_edit)
        self.cycle_time_edit = QLineEdit("10")
        self.cycle_time_edit.setValidator(QtGui.QDoubleValidator(1, 3600, 0))
        self.cycle_time_edit.setMinimumHeight(25)
        form_layout.addRow("循环时间 (s):", self.cycle_time_edit)
        self.power_source_combo = QComboBox()
        self.power_source_combo.setMinimumHeight(25)
        form_layout.addRow("测试电源:", self.power_source_combo)
        layout.addLayout(form_layout)
        button_layout = QHBoxLayout()
        ok_button = QPushButton("确定")
        ok_button.setMinimumHeight(30)
        ok_button.clicked.connect(self.accept)
        cancel_button = QPushButton("取消")
        cancel_button.setMinimumHeight(30)
        cancel_button.clicked.connect(self.reject)
        button_layout.addWidget(ok_button)
        button_layout.addWidget(cancel_button)
        layout.addLayout(button_layout)
    def set_power_source_names(self, names):
        current = self.power_source_combo.currentText()
        self.power_source_combo.clear()
        self.power_source_combo.addItems([str(name) for name in (names or [])])
        target = current or self.power_source_combo.currentText()
        if target:
            self.power_source_combo.setCurrentText(str(target))
    def apply_state(self, state: TestSettingsDialogState):
        self.start_voltage_edit.setText(state.start_voltage)
        self.target_voltage_edit.setText(state.target_voltage)
        self.voltage_step_edit.setText(state.voltage_step)
        self.step_delay_edit.setText(state.step_delay)
        self.cycle_time_edit.setText(state.cycle_time)
        self.power_source_combo.setCurrentText(state.power_source_name)
    def read_state(self) -> TestSettingsDialogState:
        return TestSettingsDialogState(
            start_voltage=self.start_voltage_edit.text(),
            target_voltage=self.target_voltage_edit.text(),
            voltage_step=self.voltage_step_edit.text(),
            step_delay=self.step_delay_edit.text(),
            cycle_time=self.cycle_time_edit.text(),
            power_source_name=self.power_source_combo.currentText(),
        )
class _LegacyCurrentStabilizationDialog(QDialog):
    """稳流参数设置对话框"""
    CURRENT_SOURCE_BY_INDEX = {
        0: "keithley",
        1: "cathode",
        2: "gate",
        3: "anode",
        4: "backup",
    }
    ALGORITHM_BY_INDEX = {
        0: "pid",
        1: "approach",
    }
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("稳流参数设置")
        self.setModal(True)
        self.setup_ui()
    def setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(10)
        layout.setContentsMargins(15, 15, 15, 15)
        form_layout = QFormLayout()
        form_layout.setVerticalSpacing(8)
        form_layout.setHorizontalSpacing(10)
        self.target_current_edit = QLineEdit("1000")
        self.target_current_edit.setValidator(QtGui.QDoubleValidator(1, 5000, 1))
        self.target_current_edit.setMinimumHeight(25)
        form_layout.addRow("目标电流 (uA):", self.target_current_edit)
        self.stability_range_edit = QLineEdit("5")
        self.stability_range_edit.setValidator(QtGui.QDoubleValidator(0.5, 10, 1))
        self.stability_range_edit.setMinimumHeight(25)
        form_layout.addRow("稳定范围 (uA):", self.stability_range_edit)
        self.start_voltage_edit = QLineEdit("100")
        self.start_voltage_edit.setValidator(QtGui.QDoubleValidator(0, 5000, 1))
        self.start_voltage_edit.setMinimumHeight(25)
        form_layout.addRow("起始电压 (V):", self.start_voltage_edit)
        self.power_source_combo = QComboBox()
        self.power_source_combo.setMinimumHeight(25)
        form_layout.addRow("稳流电源:", self.power_source_combo)
        self.current_source_combo = QComboBox()
        self.current_source_combo.addItems(["电源自身(Keithley)", "阴极", "栅极", "阳极", "收集极"])
        self.current_source_combo.setMinimumHeight(25)
        form_layout.addRow("电流数据来源:", self.current_source_combo)
        self.algorithm_combo = QComboBox()
        self.algorithm_combo.addItems(["PID", "接近算法(±范围内保持)"])
        self.algorithm_combo.setMinimumHeight(25)
        form_layout.addRow("稳流算法:", self.algorithm_combo)
        self.adjust_frequency_edit = QLineEdit("1")
        self.adjust_frequency_edit.setValidator(QtGui.QDoubleValidator(0.5, 5, 1))
        self.adjust_frequency_edit.setMinimumHeight(25)
        form_layout.addRow("调整频率 (s):", self.adjust_frequency_edit)
        self.max_adjust_voltage_edit = QLineEdit("50")
        self.max_adjust_voltage_edit.setValidator(QtGui.QDoubleValidator(1, 100, 1))
        self.max_adjust_voltage_edit.setMinimumHeight(25)
        self.pid_kp_edit = QLineEdit("0.05")
        self.pid_kp_edit.setValidator(QtGui.QDoubleValidator(0, 1000, 6))
        self.pid_kp_edit.setMinimumHeight(25)
        form_layout.addRow("PID Kp:", self.pid_kp_edit)
        self.pid_ki_edit = QLineEdit("0.01")
        self.pid_ki_edit.setValidator(QtGui.QDoubleValidator(0, 1000, 6))
        self.pid_ki_edit.setMinimumHeight(25)
        form_layout.addRow("PID Ki:", self.pid_ki_edit)
        self.pid_kd_edit = QLineEdit("0.0")
        self.pid_kd_edit.setValidator(QtGui.QDoubleValidator(0, 1000, 6))
        self.pid_kd_edit.setMinimumHeight(25)
        form_layout.addRow("PID Kd:", self.pid_kd_edit)
        form_layout.addRow("最大调整电压 (V):", self.max_adjust_voltage_edit)
        layout.addLayout(form_layout)
        button_layout = QHBoxLayout()
        ok_button = QPushButton("确定")
        ok_button.setMinimumHeight(30)
        ok_button.clicked.connect(self.accept)
        cancel_button = QPushButton("取消")
        cancel_button.setMinimumHeight(30)
        cancel_button.clicked.connect(self.reject)
        button_layout.addWidget(ok_button)
        button_layout.addWidget(cancel_button)
        layout.addLayout(button_layout)
    @classmethod
    def current_source_combo_index(cls, current_source: str) -> int:
        normalized = str(current_source or "keithley").strip().lower()
        for index, key in cls.CURRENT_SOURCE_BY_INDEX.items():
            if key == normalized:
                return index
        return 0
    @classmethod
    def combo_index_current_source(cls, index: int) -> str:
        return cls.CURRENT_SOURCE_BY_INDEX.get(int(index), "keithley")
    @classmethod
    def algorithm_combo_index(cls, algorithm: str) -> int:
        normalized = str(algorithm or "pid").strip().lower()
        for index, key in cls.ALGORITHM_BY_INDEX.items():
            if key == normalized:
                return index
        return 0
    @classmethod
    def combo_index_algorithm(cls, index: int) -> str:
        return cls.ALGORITHM_BY_INDEX.get(int(index), "pid")
    def set_power_source_names(self, names):
        current = self.power_source_combo.currentText()
        self.power_source_combo.clear()
        self.power_source_combo.addItems([str(name) for name in (names or [])])
        target = current or self.power_source_combo.currentText()
        if target:
            self.power_source_combo.setCurrentText(str(target))
    def apply_state(self, state: StabilizationSettingsDialogState):
        self.target_current_edit.setText(state.target_current)
        self.stability_range_edit.setText(state.stability_range)
        self.start_voltage_edit.setText(state.start_voltage)
        self.power_source_combo.setCurrentText(state.power_source_name)
        self.current_source_combo.setCurrentIndex(self.current_source_combo_index(state.current_source))
        if hasattr(self, "algorithm_combo"):
            self.algorithm_combo.setCurrentIndex(self.algorithm_combo_index(state.algorithm))
        self.adjust_frequency_edit.setText(state.adjust_frequency)
        self.max_adjust_voltage_edit.setText(state.max_adjust_voltage)
        self.pid_kp_edit.setText(state.pid_kp)
        self.pid_ki_edit.setText(state.pid_ki)
        self.pid_kd_edit.setText(state.pid_kd)
    def read_state(self) -> StabilizationSettingsDialogState:
        return StabilizationSettingsDialogState(
            target_current=self.target_current_edit.text(),
            stability_range=self.stability_range_edit.text(),
            start_voltage=self.start_voltage_edit.text(),
            power_source_name=self.power_source_combo.currentText(),
            current_source=self.combo_index_current_source(self.current_source_combo.currentIndex()),
            adjust_frequency=self.adjust_frequency_edit.text(),
            max_adjust_voltage=self.max_adjust_voltage_edit.text(),
            algorithm=(
                self.combo_index_algorithm(self.algorithm_combo.currentIndex())
                if hasattr(self, "algorithm_combo")
                else "pid"
            ),
            pid_kp=self.pid_kp_edit.text(),
            pid_ki=self.pid_ki_edit.text(),
            pid_kd=self.pid_kd_edit.text(),
        )
class PlotColorDialog(QDialog):
    """曲线颜色设置对话框（UI可配置）"""
    def __init__(self, parent=None, series=None, current_colors=None):
        super().__init__(parent)
        self.setWindowTitle("曲线颜色设置")
        self.setModal(True)
        self.series = series or []
        self.current_colors = current_colors or {}
        self._edits = {}
        self._setup_ui()
    def _setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(10)
        layout.setContentsMargins(15, 15, 15, 15)
        tip = QLabel("点击“选择”更改颜色（保存为 #RRGGBB）。")
        tip.setWordWrap(True)
        layout.addWidget(tip)
        grid = QGridLayout()
        grid.setHorizontalSpacing(8)
        grid.setVerticalSpacing(6)
        grid.addWidget(QLabel("曲线"), 0, 0)
        grid.addWidget(QLabel("颜色"), 0, 1)
        grid.addWidget(QLabel("操作"), 0, 2)
        row = 1
        for key, name in self.series:
            lbl = QLabel(name)
            edit = QLineEdit(self.current_colors.get(key, "#000000"))
            edit.setMinimumWidth(110)
            edit.setMaxLength(9)
            btn = QPushButton("选择")
            btn.setMinimumWidth(60)
            btn.clicked.connect(lambda checked=False, k=key: self._choose_color(k))
            self._edits[key] = edit
            grid.addWidget(lbl, row, 0)
            grid.addWidget(edit, row, 1)
            grid.addWidget(btn, row, 2)
            row += 1
        layout.addLayout(grid)
        btns = QHBoxLayout()
        btns.addStretch()
        reset_btn = QPushButton("恢复默认")
        reset_btn.clicked.connect(self._reset_defaults)
        btns.addWidget(reset_btn)
        ok_btn = QPushButton("确定")
        ok_btn.clicked.connect(self.accept)
        btns.addWidget(ok_btn)
        cancel_btn = QPushButton("取消")
        cancel_btn.clicked.connect(self.reject)
        btns.addWidget(cancel_btn)
        layout.addLayout(btns)
    def _normalize_hex(self, s: str):
        if not s:
            return None
        s = s.strip()
        if not s.startswith("#"):
            s = "#" + s
        if len(s) == 4:
            s = "#" + "".join([c * 2 for c in s[1:]])
        if len(s) != 7:
            return None
        for c in s[1:]:
            if c.lower() not in "0123456789abcdef":
                return None
        return s.upper()
    def _choose_color(self, key):
        cur = self._edits.get(key).text() if key in self._edits else "#000000"
        cur_n = self._normalize_hex(cur) or "#000000"
        color = QColorDialog.getColor(QColor(cur_n), self, f"选择颜色 - {key}")
        if color.isValid():
            self._edits[key].setText(color.name().upper())
    def _reset_defaults(self):
        defaults = {}
        try:
            if self.parent() and hasattr(self.parent(), "_default_plot_colors"):
                defaults = self.parent()._default_plot_colors()
        except Exception:
            defaults = {}
        for k, _ in self.series:
            self._edits[k].setText(str(defaults.get(k, "#000000")).upper())
    def get_colors(self):
        out = {}
        for k, _ in self.series:
            v = self._normalize_hex(self._edits[k].text())
            if v:
                out[k] = v
        return out
class _LegacyMeterSettingsDialog(QDialog):
    def __init__(self, parent=None, title="万用表设置"):
        super().__init__(parent)
        self.mw = parent
        self.setWindowTitle(title)
        self.resize(680, 260)
        self._rows = []
        self._build_ui()
    def _build_ui(self):
        layout = QVBoxLayout(self)
        top = QHBoxLayout()
        tip = QLabel("在这里设置四路万用表串口，并用滑块开关连接。系数仍在主面板调整。")
        tip.setWordWrap(True)
        top.addWidget(tip)
        refresh_btn = QPushButton("刷新串口")
        refresh_btn.clicked.connect(self.refresh_ports)
        top.addWidget(refresh_btn)
        layout.addLayout(top)
        grid = QGridLayout()
        grid.addWidget(QLabel("通道"), 0, 0)
        grid.addWidget(QLabel("串口号"), 0, 1)
        grid.addWidget(QLabel("连接"), 0, 2)
        row = 1
        for key, label in [("cathode", "阴极"), ("gate", "栅极"), ("anode", "阳极"), ("backup", "收集极")]:
            lbl = QLabel(label)
            combo = QComboBox()
            combo.setEditable(True)
            combo.addItems(self.mw.get_serial_port_list())
            try:
                combo.setCurrentText(self.mw.get_meter_port(key))
            except Exception:
                pass
            combo.currentTextChanged.connect(lambda text, k=key: self.mw.set_meter_port(k, text))
            sw = ToggleSwitch(on_text="已连接", off_text="未连接")
            is_connected = self.mw.is_meter_connected(key)
            sw.setChecked(is_connected)
            combo.setEnabled(not is_connected)
            sw.toggled.connect(lambda checked, k=key: self._toggle_meter(k, checked))
            grid.addWidget(lbl, row, 0)
            grid.addWidget(combo, row, 1)
            grid.addWidget(sw, row, 2)
            self._rows.append((key, combo, sw))
            row += 1
        layout.addLayout(grid)
        btns = QHBoxLayout()
        btns.addStretch()
        close_btn = QPushButton("关闭")
        close_btn.clicked.connect(self.accept)
        btns.addWidget(close_btn)
        layout.addLayout(btns)
    def refresh_ports(self):
        ports = self.mw.get_serial_port_list()
        for _, combo, _ in self._rows:
            current = combo.currentText()
            combo.blockSignals(True)
            combo.clear()
            combo.addItems(ports)
            combo.setCurrentText(current)
            combo.blockSignals(False)
    def _toggle_meter(self, meter_type: str, checked: bool):
        self.mw.set_meter_connection(meter_type, checked)
        for key, combo, _ in self._rows:
            if key == meter_type:
                combo.setEnabled(not checked)
                break
        self.mw.save_config_from_ui()
class VacuumSettingsDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.mw = parent
        self._initializing = False
        self.setWindowTitle("真空计设置")
        self.resize(760, 260)
        self._build_ui()
    def _build_ui(self):
        self._initializing = True
        layout = QVBoxLayout(self)
        form = QGridLayout()
        row = 0
        form.addWidget(QLabel("真空计类型"), row, 0)
        self.type_combo = QComboBox()
        self.type_combo.addItem("Leybold COMBIVAC CM52（RS232）", "CM52")
        self.type_combo.addItem("睿宝 ReBorn（Modbus RTU）", "REBORN_RTU")
        self.type_combo.addItem("Agilent XGS-600（RS232）", "AGILENT_XGS600")
        idx = self.type_combo.findData(self.mw.get_vacuum_type())
        self.type_combo.setCurrentIndex(idx if idx >= 0 else 0)
        self.type_combo.currentIndexChanged.connect(self._on_type_changed)
        form.addWidget(self.type_combo, row, 1)
        form.addWidget(QLabel("连接"), row, 2)
        self.connect_switch = ToggleSwitch(on_text="已连接", off_text="未连接")
        vacuum_connected = self.mw.is_meter_connected("vacuum")
        self.connect_switch.setChecked(vacuum_connected)
        self.connect_switch.toggled.connect(self._toggle_vacuum)
        form.addWidget(self.connect_switch, row, 3)
        row += 1
        form.addWidget(QLabel("串口号"), row, 0)
        self.port_combo = QComboBox()
        self.port_combo.setEditable(True)
        self.port_combo.addItems(self.mw.get_serial_port_list())
        self.port_combo.setCurrentText(self.mw.get_meter_port("vacuum"))
        self.port_combo.currentTextChanged.connect(lambda _text: self._save_basic())
        form.addWidget(self.port_combo, row, 1)
        self.refresh_btn = QPushButton("刷新串口")
        self.refresh_btn.clicked.connect(self._refresh_ports)
        form.addWidget(self.refresh_btn, row, 3)
        row += 1
        self.selector_label = QLabel("通道")
        form.addWidget(self.selector_label, row, 0)
        self.selector_combo = QComboBox()
        self.selector_combo.setEditable(True)
        self.selector_combo.currentTextChanged.connect(lambda _text: self._save_basic())
        form.addWidget(self.selector_combo, row, 1)
        self.baud_label = QLabel("波特率")
        form.addWidget(self.baud_label, row, 2)
        self.baud_combo = QComboBox()
        self.baud_combo.setEditable(True)
        self.baud_combo.currentTextChanged.connect(lambda _text: self._save_basic())
        form.addWidget(self.baud_combo, row, 3)
        row += 1
        self.unit_label = QLabel("控制器单位")
        form.addWidget(self.unit_label, row, 0)
        self.unit_combo = QComboBox()
        self.unit_combo.addItems(["Pa", "Torr", "mbar"])
        self.unit_combo.setCurrentText(self.mw.get_vacuum_unit())
        self.unit_combo.currentTextChanged.connect(lambda _text: self._save_basic())
        form.addWidget(self.unit_combo, row, 1)
        self.alarm_label = QLabel("告警阈值 (Pa)")
        form.addWidget(self.alarm_label, row, 2)
        self.alarm_threshold_edit = QLineEdit(self.mw.get_vacuum_alarm_max_pa())
        self.alarm_threshold_edit.setPlaceholderText("例如 1e-3")
        alarm_validator = QtGui.QDoubleValidator(0.0, 1e12, 12, self.alarm_threshold_edit)
        try:
            alarm_validator.setNotation(QtGui.QDoubleValidator.ScientificNotation)
        except Exception:
            pass
        self.alarm_threshold_edit.setValidator(alarm_validator)
        self.alarm_threshold_edit.editingFinished.connect(self._save_basic)
        form.addWidget(self.alarm_threshold_edit, row, 3)
        row += 1
        self.tip_label = QLabel()
        self.tip_label.setWordWrap(True)
        form.addWidget(self.tip_label, row, 0, 1, 4)
        layout.addLayout(form)
        layout.addStretch()
        btns = QHBoxLayout()
        btns.addStretch()
        close_btn = QPushButton("关闭")
        close_btn.clicked.connect(self.accept)
        btns.addWidget(close_btn)
        layout.addLayout(btns)
        self._on_type_changed(save=False)
        self._apply_enabled_state()
        self._initializing = False
    def _selected_type(self) -> str:
        return str(self.type_combo.currentData() or "CM52")
    def _populate_combo(self, combo: QComboBox, items, current_text: str, *, editable: bool = False, fallback: str = ""):
        combo.blockSignals(True)
        combo.clear()
        combo.setEditable(editable)
        combo.addItems([str(x) for x in items])
        target = str(current_text or fallback or "").strip()
        if target:
            combo.setCurrentText(target)
        elif combo.count() > 0:
            combo.setCurrentIndex(0)
        combo.blockSignals(False)
    def _on_type_changed(self, save: bool = True):
        vac_type = self._selected_type()
        current_selector = self.selector_combo.currentText() or self.mw.get_vacuum_channel()
        current_baud = self.baud_combo.currentText() or self.mw.get_vacuum_baudrate()
        if vac_type == "REBORN_RTU":
            self.selector_label.setText("通讯地址")
            self._populate_combo(self.selector_combo, [str(i) for i in range(1, 33)], current_selector or "1", editable=True, fallback="1")
            self._populate_combo(self.baud_combo, ["9600", "19200", "38400"], current_baud or "9600", editable=True, fallback="9600")
            self.unit_label.setVisible(False)
            self.unit_combo.setVisible(False)
            self.tip_label.setText("睿宝真空计按 Modbus RTU 读取，当前用“通讯地址”字段作为从站地址。")
        elif vac_type == "AGILENT_XGS600":
            self.selector_label.setText("读数序号")
            self._populate_combo(self.selector_combo, [str(i) for i in range(1, 13)], current_selector or "1", editable=False, fallback="1")
            self._populate_combo(self.baud_combo, ["9600", "19200"], current_baud or "9600", editable=True, fallback="9600")
            self.unit_label.setVisible(True)
            self.unit_combo.setVisible(True)
            self.tip_label.setText("Agilent XGS-600 通过 RS232 读取压力 dump，序号按控制器返回顺序选择；“控制器单位”请设置为 XGS 当前显示单位。")
        else:
            self.selector_label.setText("通道")
            self._populate_combo(self.selector_combo, [str(i) for i in range(1, 4)], current_selector or "3", editable=False, fallback="3")
            self._populate_combo(self.baud_combo, ["9600", "19200", "38400", "57600"], current_baud or "19200", editable=True, fallback="19200")
            self.unit_label.setVisible(False)
            self.unit_combo.setVisible(False)
            self.tip_label.setText("Leybold COMBIVAC CM52 使用 RPV<通道> 查询，通常 TM1/TM2/IONIVAC 对应 1/2/3。")
        self.tip_label.setText(self.tip_label.text() + "\n真空曲线颜色请在菜单栏“图线设置”中统一调整。")
        self._apply_enabled_state()
        if save and not self._initializing:
            self._save_basic()
    def _apply_enabled_state(self):
        connected = self.connect_switch.isChecked()
        enable_edit = not connected
        self.type_combo.setEnabled(enable_edit)
        self.port_combo.setEnabled(enable_edit)
        self.selector_combo.setEnabled(enable_edit)
        self.baud_combo.setEnabled(enable_edit)
        self.unit_combo.setEnabled(enable_edit and self.unit_combo.isVisible())
        self.alarm_threshold_edit.setEnabled(True)
    def _refresh_ports(self):
        current = self.port_combo.currentText()
        self.port_combo.blockSignals(True)
        self.port_combo.clear()
        self.port_combo.addItems(self.mw.get_serial_port_list())
        self.port_combo.setCurrentText(current)
        self.port_combo.blockSignals(False)
    def _save_basic(self):
        if self._initializing:
            return
        self.mw.set_vacuum_type(self._selected_type())
        self.mw.set_meter_port("vacuum", self.port_combo.currentText())
        self.mw.set_vacuum_channel(self.selector_combo.currentText())
        self.mw.set_vacuum_baudrate(self.baud_combo.currentText())
        self.mw.set_vacuum_unit(self.unit_combo.currentText())
        self._save_alarm_threshold()
        self.mw.save_config_from_ui()

    def _save_alarm_threshold(self):
        raw_text = self.alarm_threshold_edit.text().strip()
        if not raw_text:
            raw_text = self.mw.get_vacuum_alarm_max_pa()
            self.alarm_threshold_edit.setText(str(raw_text))
        try:
            numeric = float(raw_text)
            if numeric <= 0:
                raise ValueError("threshold must be positive")
        except Exception:
            fallback = self.mw.get_vacuum_alarm_max_pa()
            self.alarm_threshold_edit.setText(str(fallback))
            try:
                self.mw.log_message("真空告警阈值无效，已恢复为上次有效值")
            except Exception:
                pass
            return
        normalized = f"{numeric:g}"
        self.alarm_threshold_edit.setText(normalized)
        self.mw.set_vacuum_alarm_max_pa(normalized)
    def _toggle_vacuum(self, checked: bool):
        self._save_basic()
        self.mw.set_meter_connection("vacuum", checked)
        self._apply_enabled_state()
        self.mw.save_config_from_ui()
class PowerSettingsDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.mw = parent
        self._serial_ports_cache = []
        self._gpib_resources_cache = []
        self.setWindowTitle("电源设置")
        self.resize(860, 420)
        self._build_ui()
        self.rebuild_rows()
    def _build_ui(self):
        layout = QVBoxLayout(self)
        top = QHBoxLayout()
        tip = QLabel("可自定义电源名称，并按名称在升压测试/稳流测试中绑定。当前版本 HAPS06 保持单连接，Keithley 类电源（248 / 2290-5 / 2290E-5）可按名称同时连接多台。")
        tip.setWordWrap(True)
        top.addWidget(tip)
        self.refresh_btn = QPushButton("刷新端口")
        self.refresh_btn.clicked.connect(self._refresh_ports_and_rebuild)
        top.addWidget(self.refresh_btn)
        self.add_btn = QPushButton("增加电源")
        self.add_btn.clicked.connect(self._add_device)
        top.addWidget(self.add_btn)
        layout.addLayout(top)
        self.scroll = QScrollArea()
        self.scroll.setWidgetResizable(True)
        self.container = QWidget()
        self.rows_layout = QVBoxLayout(self.container)
        self.rows_layout.setContentsMargins(6, 6, 6, 6)
        self.rows_layout.setSpacing(8)
        self.scroll.setWidget(self.container)
        layout.addWidget(self.scroll)
        btns = QHBoxLayout()
        btns.addStretch()
        close_btn = QPushButton("关闭")
        close_btn.clicked.connect(self.accept)
        btns.addWidget(close_btn)
        layout.addLayout(btns)
    def _refresh_available_ports(self):
        try:
            self._serial_ports_cache = list(self.mw.get_serial_port_list())
        except Exception:
            self._serial_ports_cache = []
        try:
            self._gpib_resources_cache = list(self.mw.get_gpib_resource_list())
        except Exception:
            self._gpib_resources_cache = []
    def _refresh_ports_and_rebuild(self):
        self._refresh_available_ports()
        self.rebuild_rows(use_cached_ports=True)
    def rebuild_rows(self, use_cached_ports: bool = False):
        if not use_cached_ports:
            self._refresh_available_ports()
        while self.rows_layout.count():
            item = self.rows_layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()
        for index, device in enumerate(self.mw.power_devices):
            self.rows_layout.addWidget(self._make_row(index, device))
        self.rows_layout.addStretch()
    def _make_row(self, index: int, device: dict):
        box = QGroupBox(f"电源 {index + 1}")
        grid = QGridLayout(box)
        grid.addWidget(QLabel("电源名称"), 0, 0)
        name_edit = QLineEdit(str(device.get("name", "")))
        grid.addWidget(name_edit, 0, 1)
        grid.addWidget(QLabel("电源类型"), 0, 2)
        type_combo = QComboBox()
        type_combo.addItem("HAPS06", "HAPS06")
        type_combo.addItem("Keithley（248 / 2290-5 / 2290E-5）", "Keithley 248")
        current_type = self.mw.normalize_power_type(device.get("type", "HAPS06"))
        current_index = type_combo.findData(current_type)
        if current_index < 0:
            current_index = 0
        type_combo.setCurrentIndex(current_index)
        type_combo.setToolTip("Keithley 选项兼容 248、2290-5 和 2290E-5。")
        grid.addWidget(type_combo, 0, 3)
        grid.addWidget(QLabel("端口/GPIB"), 1, 0)
        addr_combo = QComboBox()
        addr_combo.setEditable(True)
        grid.addWidget(addr_combo, 1, 1)
        grid.addWidget(QLabel("波特率"), 1, 2)
        baud_combo = QComboBox()
        baud_combo.setEditable(True)
        baud_combo.addItems(["9600", "19200", "38400", "57600"])
        baud_combo.setCurrentText(str(device.get("baudrate", "9600")))
        grid.addWidget(baud_combo, 1, 3)
        grid.addWidget(QLabel("连接开关"), 2, 0)
        switch = ToggleSwitch(on_text="已连接", off_text="未连接")
        switch.setChecked(self.mw.is_power_device_connected(device.get("name", "")))
        grid.addWidget(switch, 2, 1)
        status_lbl = QLabel(self.mw.get_power_device_status_text(device.get("name", "")))
        status_lbl.setWordWrap(True)
        grid.addWidget(status_lbl, 2, 2)
        remove_btn = QPushButton("删除")
        remove_btn.clicked.connect(lambda: self._remove_device(index))
        grid.addWidget(remove_btn, 2, 3)
        def refresh_address_options(current_type=None):
            selected_type = current_type if current_type is not None else (type_combo.currentData() or type_combo.currentText())
            typ = self.mw.normalize_power_type(selected_type)
            options = (self._serial_ports_cache if typ == "HAPS06" else self._gpib_resources_cache)
            current_value = str(self.mw.power_devices[index].get("address", ""))
            addr_combo.blockSignals(True)
            addr_combo.clear()
            addr_combo.addItems(options)
            addr_combo.setCurrentText(current_value)
            addr_combo.blockSignals(False)
            is_connected = self.mw.is_power_device_connected(self.mw.power_devices[index].get("name", ""))
            name_edit.setEnabled(not is_connected)
            type_combo.setEnabled(not is_connected)
            addr_combo.setEnabled(not is_connected)
            baud_combo.setEnabled((typ == "HAPS06") and (not is_connected))
            remove_btn.setEnabled(not is_connected)
        def on_name_finished():
            self.mw.rename_power_device(index, name_edit.text())
            self.mw.save_config_from_ui()
            self.rebuild_rows()
        def on_type_changed(_index):
            selected_type = type_combo.currentData() or type_combo.currentText()
            self.mw.update_power_device_field(index, "type", selected_type)
            refresh_address_options(selected_type)
            status_lbl.setText(self.mw.get_power_device_status_text(self.mw.power_devices[index].get("name", "")))
            self.mw.save_config_from_ui()
        def on_address_changed(text):
            self.mw.update_power_device_field(index, "address", text)
            self.mw.save_config_from_ui()
            status_lbl.setText(self.mw.get_power_device_status_text(self.mw.power_devices[index].get("name", "")))
        def on_baud_changed(text):
            self.mw.update_power_device_field(index, "baudrate", text)
            self.mw.save_config_from_ui()
        def on_switch_toggled(checked):
            dev_name = self.mw.power_devices[index].get("name", "")
            if checked:
                self.mw.connect_named_power_device(dev_name)
            else:
                self.mw.disconnect_named_power_device(dev_name)
            status_lbl.setText(self.mw.get_power_device_status_text(dev_name))
            self.mw.save_config_from_ui()
            self.rebuild_rows()
        name_edit.editingFinished.connect(on_name_finished)
        type_combo.currentIndexChanged.connect(on_type_changed)
        addr_combo.currentTextChanged.connect(on_address_changed)
        baud_combo.currentTextChanged.connect(on_baud_changed)
        switch.toggled.connect(on_switch_toggled)
        refresh_address_options(type_combo.currentData() or type_combo.currentText())
        return box
    def _add_device(self):
        self.mw.add_power_device()
        self.mw.save_config_from_ui()
        self.rebuild_rows()
    def _remove_device(self, index: int):
        self.mw.remove_power_device(index)
        self.mw.save_config_from_ui()
        self.rebuild_rows()
class RemoteInfluxSettingsDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.mw = parent
        self.setWindowTitle("远程控制与 InfluxDB 设置")
        self.resize(760, 420)
        self._build_ui()
        self.refresh_states()
    def _build_ui(self):
        layout = QVBoxLayout(self)
        tip = QLabel("远程控制(Web)和 InfluxDB 均改为手动开关控制。程序重启后默认不自动启动。")
        tip.setWordWrap(True)
        layout.addWidget(tip)
        remote_box = QGroupBox("远程控制(Web)")
        remote_form = QGridLayout(remote_box)
        remote_form.addWidget(QLabel("IP"), 0, 0)
        self.remote_host_edit = QLineEdit(self.mw.get_remote_host())
        remote_form.addWidget(self.remote_host_edit, 0, 1)
        remote_form.addWidget(QLabel("端口"), 0, 2)
        self.remote_port_edit = QLineEdit(str(self.mw.get_remote_port()))
        self.remote_port_edit.setValidator(QtGui.QIntValidator(1, 65535))
        remote_form.addWidget(self.remote_port_edit, 0, 3)
        remote_form.addWidget(QLabel("开关"), 1, 0)
        self.remote_switch = ToggleSwitch(on_text="运行中", off_text="已关闭")
        self.remote_switch.toggled.connect(self._toggle_remote)
        remote_form.addWidget(self.remote_switch, 1, 1)
        self.remote_status = QLabel("")
        self.remote_status.setWordWrap(True)
        remote_form.addWidget(self.remote_status, 1, 2, 1, 2)
        layout.addWidget(remote_box)
        influx_box = QGroupBox("InfluxDB")
        influx_form = QGridLayout(influx_box)
        influx_form.addWidget(QLabel("IP"), 0, 0)
        self.influx_host_edit = QLineEdit(self.mw.get_influx_host())
        influx_form.addWidget(self.influx_host_edit, 0, 1)
        influx_form.addWidget(QLabel("端口"), 0, 2)
        self.influx_port_edit = QLineEdit(str(self.mw.get_influx_port()))
        self.influx_port_edit.setValidator(QtGui.QIntValidator(1, 65535))
        influx_form.addWidget(self.influx_port_edit, 0, 3)
        influx_form.addWidget(QLabel("Org"), 1, 0)
        self.influx_org_edit = QLineEdit(self.mw.get_influx_org())
        influx_form.addWidget(self.influx_org_edit, 1, 1)
        influx_form.addWidget(QLabel("Bucket"), 1, 2)
        self.influx_bucket_edit = QLineEdit(self.mw.get_influx_bucket())
        influx_form.addWidget(self.influx_bucket_edit, 1, 3)
        influx_form.addWidget(QLabel("Token"), 2, 0)
        self.influx_token_edit = QLineEdit(self.mw.get_influx_token())
        self.influx_token_edit.setEchoMode(QLineEdit.Password)
        influx_form.addWidget(self.influx_token_edit, 2, 1, 1, 3)
        influx_form.addWidget(QLabel("开关"), 3, 0)
        self.influx_switch = ToggleSwitch(on_text="运行中", off_text="已关闭")
        self.influx_switch.toggled.connect(self._toggle_influx)
        influx_form.addWidget(self.influx_switch, 3, 1)
        self.influx_status = QLabel("")
        self.influx_status.setWordWrap(True)
        influx_form.addWidget(self.influx_status, 3, 2, 1, 2)
        layout.addWidget(influx_box)
        for widget in [
            self.remote_host_edit, self.remote_port_edit,
            self.influx_host_edit, self.influx_port_edit,
            self.influx_org_edit, self.influx_bucket_edit,
            self.influx_token_edit,
        ]:
            widget.editingFinished.connect(self._save_all)
        btns = QHBoxLayout()
        btns.addStretch()
        close_btn = QPushButton("关闭")
        close_btn.clicked.connect(self.accept)
        btns.addWidget(close_btn)
        layout.addLayout(btns)
    def _save_all(self):
        self.mw.set_remote_host(self.remote_host_edit.text().strip())
        self.mw.set_remote_port(self.remote_port_edit.text().strip())
        self.mw.set_influx_host_port(self.influx_host_edit.text().strip(), self.influx_port_edit.text().strip())
        self.mw.set_influx_org(self.influx_org_edit.text().strip())
        self.mw.set_influx_bucket(self.influx_bucket_edit.text().strip())
        self.mw.set_influx_token(self.influx_token_edit.text())
        self.mw.save_config_from_ui()
        self.refresh_states()
    def _toggle_remote(self, checked: bool):
        self._save_all()
        self.mw.set_remote_control_enabled(checked)
        self.refresh_states()
    def _toggle_influx(self, checked: bool):
        self._save_all()
        self.mw.set_influx_enabled(checked)
        self.refresh_states()
    def refresh_states(self):
        remote_running = self.mw.is_remote_control_enabled()
        influx_running = self.mw.is_influx_enabled()
        self.remote_switch.blockSignals(True)
        self.influx_switch.blockSignals(True)
        self.remote_switch.setChecked(remote_running)
        self.influx_switch.setChecked(influx_running)
        self.remote_switch.blockSignals(False)
        self.influx_switch.blockSignals(False)
        self.remote_status.setText(self.mw.get_remote_status_text())
        self.influx_status.setText(self.mw.get_influx_status_text())


class TestSettingsDialog(QDialog):
    """测试参数设置对话框。"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("测试参数设置")
        self.setModal(True)
        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(10)
        layout.setContentsMargins(15, 15, 15, 15)

        form_layout = QFormLayout()
        form_layout.setVerticalSpacing(8)
        form_layout.setHorizontalSpacing(10)

        self.start_voltage_edit = QLineEdit("0")
        self.start_voltage_edit.setValidator(QtGui.QDoubleValidator(0, 10000, 3))
        self.start_voltage_edit.setMinimumHeight(25)
        form_layout.addRow("起始电压 (V):", self.start_voltage_edit)

        self.target_voltage_edit = QLineEdit("1000")
        self.target_voltage_edit.setValidator(QtGui.QDoubleValidator(0, 10000, 3))
        self.target_voltage_edit.setMinimumHeight(25)
        form_layout.addRow("目标电压 (V):", self.target_voltage_edit)

        self.voltage_step_edit = QLineEdit("10")
        self.voltage_step_edit.setValidator(QtGui.QDoubleValidator(0.001, 1000, 3))
        self.voltage_step_edit.setMinimumHeight(25)
        form_layout.addRow("电压步进 (V):", self.voltage_step_edit)

        self.step_delay_edit = QLineEdit("1")
        self.step_delay_edit.setValidator(QtGui.QDoubleValidator(0.1, 3600, 3))
        self.step_delay_edit.setMinimumHeight(25)
        form_layout.addRow("步进延时 (s):", self.step_delay_edit)

        self.cycle_time_edit = QLineEdit("10")
        self.cycle_time_edit.setValidator(QtGui.QDoubleValidator(0, 3600, 3))
        self.cycle_time_edit.setMinimumHeight(25)
        form_layout.addRow("循环时间 (s):", self.cycle_time_edit)

        self.power_source_combo = QComboBox()
        self.power_source_combo.setMinimumHeight(25)
        form_layout.addRow("测试电源:", self.power_source_combo)
        layout.addLayout(form_layout)

        button_layout = QHBoxLayout()
        preset_save_button = QPushButton("另存为预设")
        preset_save_button.setMinimumHeight(30)
        preset_save_button.clicked.connect(self._save_preset)
        preset_load_button = QPushButton("加载预设")
        preset_load_button.setMinimumHeight(30)
        preset_load_button.clicked.connect(self._load_preset)
        ok_button = QPushButton("确定")
        ok_button.setMinimumHeight(30)
        ok_button.clicked.connect(self.accept)
        cancel_button = QPushButton("取消")
        cancel_button.setMinimumHeight(30)
        cancel_button.clicked.connect(self.reject)
        button_layout.addWidget(preset_save_button)
        button_layout.addWidget(preset_load_button)
        button_layout.addWidget(ok_button)
        button_layout.addWidget(cancel_button)
        layout.addLayout(button_layout)

    def set_power_source_names(self, names):
        current = self.power_source_combo.currentText()
        self.power_source_combo.clear()
        self.power_source_combo.addItems([str(name) for name in (names or [])])
        if current:
            self.power_source_combo.setCurrentText(str(current))

    def apply_state(self, state: TestSettingsDialogState):
        self.start_voltage_edit.setText(state.start_voltage)
        self.target_voltage_edit.setText(state.target_voltage)
        self.voltage_step_edit.setText(state.voltage_step)
        self.step_delay_edit.setText(state.step_delay)
        self.cycle_time_edit.setText(state.cycle_time)
        self.power_source_combo.setCurrentText(state.power_source_name)

    def read_state(self) -> TestSettingsDialogState:
        return TestSettingsDialogState(
            start_voltage=self.start_voltage_edit.text(),
            target_voltage=self.target_voltage_edit.text(),
            voltage_step=self.voltage_step_edit.text(),
            step_delay=self.step_delay_edit.text(),
            cycle_time=self.cycle_time_edit.text(),
            power_source_name=self.power_source_combo.currentText(),
        )

    def _settings_service(self):
        parent = self.parent()
        return getattr(parent, "settings_service", None)

    def _save_preset(self):
        service = self._settings_service()
        if service is not None:
            service.save_test_preset_from_dialog(self.read_state())

    def _load_preset(self):
        service = self._settings_service()
        if service is None:
            return
        state = service.load_test_preset_for_dialog()
        if state is not None:
            self.apply_state(state)


class CurrentStabilizationDialog(QDialog):
    """稳流参数设置对话框。"""

    CURRENT_SOURCE_BY_INDEX = {
        0: "keithley",
        1: "cathode",
        2: "gate",
        3: "anode",
        4: "backup",
    }
    ALGORITHM_BY_INDEX = {
        0: "pid",
        1: "approach",
    }

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("稳流参数设置")
        self.setModal(True)
        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(10)
        layout.setContentsMargins(15, 15, 15, 15)

        form_layout = QFormLayout()
        form_layout.setVerticalSpacing(8)
        form_layout.setHorizontalSpacing(10)

        self.target_current_edit = QLineEdit("1000")
        self.target_current_edit.setValidator(QtGui.QDoubleValidator(0, 1_000_000_000, 3))
        self.target_current_edit.setMinimumHeight(25)
        form_layout.addRow("目标值 (uA / mV):", self.target_current_edit)

        self.stability_range_edit = QLineEdit("5")
        self.stability_range_edit.setValidator(QtGui.QDoubleValidator(0, 1_000_000_000, 3))
        self.stability_range_edit.setMinimumHeight(25)
        form_layout.addRow("稳定范围 (uA / mV):", self.stability_range_edit)

        self.start_voltage_edit = QLineEdit("100")
        self.start_voltage_edit.setValidator(QtGui.QDoubleValidator(-10000, 10000, 3))
        self.start_voltage_edit.setMinimumHeight(25)
        form_layout.addRow("起始电压 (V):", self.start_voltage_edit)

        self.power_source_combo = QComboBox()
        self.power_source_combo.setMinimumHeight(25)
        form_layout.addRow("稳流电源:", self.power_source_combo)

        self.current_source_combo = QComboBox()
        self.current_source_combo.addItems(["电源自身(Keithley)", "阴极", "栅极", "阳极", "收集极"])
        self.current_source_combo.setMinimumHeight(25)
        form_layout.addRow("反馈数据来源:", self.current_source_combo)

        self.algorithm_combo = QComboBox()
        self.algorithm_combo.addItems(["PID", "接近算法(1V步进)"])
        self.algorithm_combo.setMinimumHeight(25)
        form_layout.addRow("稳流算法:", self.algorithm_combo)

        self.adjust_frequency_edit = QLineEdit("1")
        self.adjust_frequency_edit.setValidator(QtGui.QDoubleValidator(0.1, 60, 3))
        self.adjust_frequency_edit.setMinimumHeight(25)
        form_layout.addRow("调整频率 (s):", self.adjust_frequency_edit)

        self.max_adjust_voltage_edit = QLineEdit("50")
        self.max_adjust_voltage_edit.setValidator(QtGui.QDoubleValidator(0, 10000, 3))
        self.max_adjust_voltage_edit.setMinimumHeight(25)
        self.pid_kp_edit = QLineEdit("0.05")
        self.pid_kp_edit.setValidator(QtGui.QDoubleValidator(0, 1000, 6))
        self.pid_kp_edit.setMinimumHeight(25)
        form_layout.addRow("PID Kp:", self.pid_kp_edit)
        self.pid_ki_edit = QLineEdit("0.01")
        self.pid_ki_edit.setValidator(QtGui.QDoubleValidator(0, 1000, 6))
        self.pid_ki_edit.setMinimumHeight(25)
        form_layout.addRow("PID Ki:", self.pid_ki_edit)
        self.pid_kd_edit = QLineEdit("0.0")
        self.pid_kd_edit.setValidator(QtGui.QDoubleValidator(0, 1000, 6))
        self.pid_kd_edit.setMinimumHeight(25)
        form_layout.addRow("PID Kd:", self.pid_kd_edit)
        form_layout.addRow("最大调压步进 (V):", self.max_adjust_voltage_edit)

        layout.addLayout(form_layout)

        button_layout = QHBoxLayout()
        preset_save_button = QPushButton("另存为预设")
        preset_save_button.setMinimumHeight(30)
        preset_save_button.clicked.connect(self._save_preset)
        preset_load_button = QPushButton("加载预设")
        preset_load_button.setMinimumHeight(30)
        preset_load_button.clicked.connect(self._load_preset)
        ok_button = QPushButton("确定")
        ok_button.setMinimumHeight(30)
        ok_button.clicked.connect(self.accept)
        cancel_button = QPushButton("取消")
        cancel_button.setMinimumHeight(30)
        cancel_button.clicked.connect(self.reject)
        button_layout.addWidget(preset_save_button)
        button_layout.addWidget(preset_load_button)
        button_layout.addWidget(ok_button)
        button_layout.addWidget(cancel_button)
        layout.addLayout(button_layout)

    @classmethod
    def current_source_combo_index(cls, current_source: str) -> int:
        normalized = str(current_source or "keithley").strip().lower()
        for index, key in cls.CURRENT_SOURCE_BY_INDEX.items():
            if key == normalized:
                return index
        return 0

    @classmethod
    def combo_index_current_source(cls, index: int) -> str:
        return cls.CURRENT_SOURCE_BY_INDEX.get(int(index), "keithley")

    @classmethod
    def algorithm_combo_index(cls, algorithm: str) -> int:
        normalized = str(algorithm or "pid").strip().lower()
        for index, key in cls.ALGORITHM_BY_INDEX.items():
            if key == normalized:
                return index
        return 0

    @classmethod
    def combo_index_algorithm(cls, index: int) -> str:
        return cls.ALGORITHM_BY_INDEX.get(int(index), "pid")

    def set_power_source_names(self, names):
        current = self.power_source_combo.currentText()
        self.power_source_combo.clear()
        self.power_source_combo.addItems([str(name) for name in (names or [])])
        if current:
            self.power_source_combo.setCurrentText(str(current))

    def apply_state(self, state: StabilizationSettingsDialogState):
        self.target_current_edit.setText(state.target_current)
        self.stability_range_edit.setText(state.stability_range)
        self.start_voltage_edit.setText(state.start_voltage)
        self.power_source_combo.setCurrentText(state.power_source_name)
        self.current_source_combo.setCurrentIndex(self.current_source_combo_index(state.current_source))
        self.algorithm_combo.setCurrentIndex(self.algorithm_combo_index(state.algorithm))
        self.adjust_frequency_edit.setText(state.adjust_frequency)
        self.max_adjust_voltage_edit.setText(state.max_adjust_voltage)
        self.pid_kp_edit.setText(state.pid_kp)
        self.pid_ki_edit.setText(state.pid_ki)
        self.pid_kd_edit.setText(state.pid_kd)

    def read_state(self) -> StabilizationSettingsDialogState:
        return StabilizationSettingsDialogState(
            target_current=self.target_current_edit.text(),
            stability_range=self.stability_range_edit.text(),
            start_voltage=self.start_voltage_edit.text(),
            power_source_name=self.power_source_combo.currentText(),
            current_source=self.combo_index_current_source(self.current_source_combo.currentIndex()),
            adjust_frequency=self.adjust_frequency_edit.text(),
            max_adjust_voltage=self.max_adjust_voltage_edit.text(),
            algorithm=self.combo_index_algorithm(self.algorithm_combo.currentIndex()),
            pid_kp=self.pid_kp_edit.text(),
            pid_ki=self.pid_ki_edit.text(),
            pid_kd=self.pid_kd_edit.text(),
        )

    def _settings_service(self):
        parent = self.parent()
        return getattr(parent, "settings_service", None)

    def _save_preset(self):
        service = self._settings_service()
        if service is not None:
            service.save_stabilization_preset_from_dialog(self.read_state())

    def _load_preset(self):
        service = self._settings_service()
        if service is None:
            return
        state = service.load_stabilization_preset_for_dialog()
        if state is not None:
            self.apply_state(state)

class PlotSettingsDialog(QDialog):
    """图线设置对话框。"""

    def __init__(self, parent=None, series=None, current_colors=None, current_max_points=None):
        super().__init__(parent)
        self.setWindowTitle("图线设置")
        self.setModal(True)
        self.series = series or []
        self.current_colors = current_colors or {}
        self.current_max_points = current_max_points
        self._edits = {}
        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(10)
        layout.setContentsMargins(15, 15, 15, 15)

        tip = QLabel("统一设置曲线颜色和图表点位数。点位数留空或填 0 表示不限制。")
        tip.setWordWrap(True)
        layout.addWidget(tip)

        form = QFormLayout()
        form.setVerticalSpacing(8)
        form.setHorizontalSpacing(12)
        self.max_points_edit = QLineEdit("" if self.current_max_points is None else str(self.current_max_points))
        self.max_points_edit.setPlaceholderText("0 或留空表示不限制")
        self.max_points_edit.setValidator(QtGui.QIntValidator(0, 2147483647))
        form.addRow("图表点位数:", self.max_points_edit)
        layout.addLayout(form)

        grid = QGridLayout()
        grid.setHorizontalSpacing(8)
        grid.setVerticalSpacing(6)
        grid.addWidget(QLabel("图线"), 0, 0)
        grid.addWidget(QLabel("颜色"), 0, 1)
        grid.addWidget(QLabel("操作"), 0, 2)
        row = 1
        for key, name in self.series:
            label = QLabel(name)
            edit = QLineEdit(self.current_colors.get(key, "#000000"))
            edit.setMinimumWidth(110)
            edit.setMaxLength(9)
            button = QPushButton("选择")
            button.setMinimumWidth(60)
            button.clicked.connect(lambda checked=False, current_key=key: self._choose_color(current_key))
            self._edits[key] = edit
            grid.addWidget(label, row, 0)
            grid.addWidget(edit, row, 1)
            grid.addWidget(button, row, 2)
            row += 1
        layout.addLayout(grid)

        buttons = QHBoxLayout()
        buttons.addStretch()
        reset_button = QPushButton("恢复默认")
        reset_button.clicked.connect(self._reset_defaults)
        buttons.addWidget(reset_button)
        ok_button = QPushButton("确定")
        ok_button.clicked.connect(self.accept)
        buttons.addWidget(ok_button)
        cancel_button = QPushButton("取消")
        cancel_button.clicked.connect(self.reject)
        buttons.addWidget(cancel_button)
        layout.addLayout(buttons)

    def _normalize_hex(self, text: str):
        if not text:
            return None
        value = text.strip()
        if not value.startswith("#"):
            value = "#" + value
        if len(value) == 4:
            value = "#" + "".join(ch * 2 for ch in value[1:])
        if len(value) != 7:
            return None
        for char in value[1:]:
            if char.lower() not in "0123456789abcdef":
                return None
        return value.upper()

    def _choose_color(self, key):
        current = self._edits.get(key).text() if key in self._edits else "#000000"
        normalized = self._normalize_hex(current) or "#000000"
        color = QColorDialog.getColor(QColor(normalized), self, f"选择颜色 - {key}")
        if color.isValid():
            self._edits[key].setText(color.name().upper())

    def _reset_defaults(self):
        defaults = {}
        try:
            if self.parent() and hasattr(self.parent(), "_default_plot_colors"):
                defaults = self.parent()._default_plot_colors()
        except Exception:
            defaults = {}
        for key, _ in self.series:
            self._edits[key].setText(str(defaults.get(key, "#000000")).upper())
        self.max_points_edit.setText("")

    def get_colors(self):
        colors = {}
        for key, _ in self.series:
            value = self._normalize_hex(self._edits[key].text())
            if value:
                colors[key] = value
        return colors

    def get_max_points(self):
        text = self.max_points_edit.text().strip()
        if not text:
            return None
        try:
            numeric = int(float(text))
        except Exception:
            return None
        return numeric if numeric > 0 else None

    def get_settings(self):
        return self.get_colors(), self.get_max_points()


PlotColorDialog = PlotSettingsDialog


class MeterSettingsDialog(QDialog):
    """万用表设备设置对话框。"""

    CHANNELS = [
        ("cathode", "阴极"),
        ("gate", "栅极"),
        ("anode", "阳极"),
        ("backup", "收集极"),
    ]

    def __init__(self, parent=None, title="万用表设置"):
        super().__init__(parent)
        self.mw = parent
        self.setWindowTitle(title)
        self.setFixedSize(680, 430)
        self._rows = []
        self._build_ui()

    def _build_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(14)

        tip = QLabel("在这里为四路万用表选择串口或 HID 设备，并使用开关连接。长设备标识会自动缩略显示，完整信息可在提示中查看。")
        tip.setWordWrap(True)
        layout.addWidget(tip)

        toolbar = QHBoxLayout()
        section_label = QLabel("通道配置")
        section_font = section_label.font()
        section_font.setBold(True)
        section_label.setFont(section_font)
        toolbar.addWidget(section_label)
        toolbar.addStretch()
        refresh_btn = QPushButton("刷新设备")
        refresh_btn.setMinimumWidth(120)
        refresh_btn.clicked.connect(self.refresh_ports)
        toolbar.addWidget(refresh_btn)
        layout.addLayout(toolbar)

        grid = QGridLayout()
        grid.setColumnStretch(1, 1)
        grid.setHorizontalSpacing(14)
        grid.setVerticalSpacing(16)

        header_channel = QLabel("通道")
        header_device = QLabel("设备")
        header_connect = QLabel("连接")
        for header in (header_channel, header_device, header_connect):
            font = header.font()
            font.setBold(True)
            header.setFont(font)

        grid.addWidget(header_channel, 0, 0)
        grid.addWidget(header_device, 0, 1)
        grid.addWidget(header_connect, 0, 2, alignment=Qt.AlignCenter)

        row = 1
        for key, label in self.CHANNELS:
            name_label = QLabel(label)
            name_label.setMinimumWidth(72)
            name_label.setFixedHeight(36)
            combo = QComboBox()
            combo.setMinimumWidth(420)
            combo.setFixedHeight(34)
            combo.setSizeAdjustPolicy(QComboBox.AdjustToMinimumContentsLengthWithIcon)
            combo.setMinimumContentsLength(18)
            try:
                combo.view().setTextElideMode(Qt.ElideMiddle)
            except Exception:
                pass
            self._populate_meter_combo(combo, self.mw.get_meter_port(key))
            combo.currentIndexChanged.connect(lambda _index, meter_key=key, meter_combo=combo: self._on_combo_changed(meter_key, meter_combo))

            switch = ToggleSwitch(on_text="已连接", off_text="未连接")
            is_connected = self.mw.is_meter_connected(key)
            switch.setChecked(is_connected)
            combo.setEnabled(not is_connected)
            switch.toggled.connect(lambda checked, meter_key=key: self._toggle_meter(meter_key, checked))

            grid.addWidget(name_label, row, 0)
            grid.addWidget(combo, row, 1)
            grid.addWidget(switch, row, 2, alignment=Qt.AlignCenter)
            grid.setRowMinimumHeight(row, 44)
            self._rows.append((key, combo, switch))
            row += 1

        layout.addLayout(grid)
        layout.addStretch(1)

        buttons = QHBoxLayout()
        buttons.setContentsMargins(0, 4, 0, 0)
        buttons.addStretch()
        close_btn = QPushButton("关闭")
        close_btn.setMinimumWidth(88)
        close_btn.clicked.connect(self.accept)
        buttons.addWidget(close_btn)
        layout.addLayout(buttons)

    def _current_meter_options(self):
        try:
            return list(self.mw.get_meter_device_options())
        except Exception:
            return []

    def _populate_meter_combo(self, combo: QComboBox, current_device_id: str):
        options = self._current_meter_options()
        combo.blockSignals(True)
        combo.clear()
        selected_index = -1
        for index, option in enumerate(options):
            combo.addItem(option.label, option.device_id)
            combo.setItemData(index, option.tooltip, Qt.ToolTipRole)
            if option.device_id == current_device_id:
                selected_index = index
        if selected_index >= 0:
            combo.setCurrentIndex(selected_index)
            combo.setToolTip(combo.itemData(selected_index, Qt.ToolTipRole) or "")
        elif current_device_id:
            fallback_option = self.mw.get_meter_device_option(current_device_id)
            if fallback_option is not None:
                combo.addItem(fallback_option.label, fallback_option.device_id)
                index = combo.count() - 1
                combo.setItemData(index, fallback_option.tooltip, Qt.ToolTipRole)
                combo.setCurrentIndex(index)
                combo.setToolTip(fallback_option.tooltip)
        combo.blockSignals(False)

    def refresh_ports(self):
        for key, combo, _switch in self._rows:
            self._populate_meter_combo(combo, self.mw.get_meter_port(key))

    def _on_combo_changed(self, meter_type: str, combo: QComboBox):
        device_id = combo.currentData()
        if device_id is None:
            return
        self.mw.set_meter_port(meter_type, str(device_id))
        combo.setToolTip(str(combo.currentData(Qt.ToolTipRole) or combo.itemData(combo.currentIndex(), Qt.ToolTipRole) or ""))

    def _toggle_meter(self, meter_type: str, checked: bool):
        self.mw.set_meter_connection(meter_type, checked)
        for key, combo, _switch in self._rows:
            if key == meter_type:
                combo.setEnabled(not checked)
                break
        self.mw.save_config_from_ui()

