from __future__ import annotations

from .common import *

class TestSettingsDialog(QDialog):
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

        # 创建表单布局
        form_layout = QFormLayout()
        form_layout.setVerticalSpacing(8)
        form_layout.setHorizontalSpacing(10)

        # 起始电压
        self.start_voltage_edit = QLineEdit("0")
        self.start_voltage_edit.setValidator(QtGui.QDoubleValidator(0, 10000, 1))
        self.start_voltage_edit.setMinimumHeight(25)
        form_layout.addRow("起始电压 (V):", self.start_voltage_edit)

        # 目标电压
        self.target_voltage_edit = QLineEdit("1000")
        self.target_voltage_edit.setValidator(QtGui.QDoubleValidator(0, 10000, 1))
        self.target_voltage_edit.setMinimumHeight(25)
        form_layout.addRow("目标电压 (V):", self.target_voltage_edit)

        # 电压增幅
        self.voltage_step_edit = QLineEdit("10")
        self.voltage_step_edit.setValidator(QtGui.QDoubleValidator(0.1, 1000, 1))
        self.voltage_step_edit.setMinimumHeight(25)
        form_layout.addRow("电压增幅 (V):", self.voltage_step_edit)

        # 升压延迟
        self.step_delay_edit = QLineEdit("1")
        self.step_delay_edit.setValidator(QtGui.QDoubleValidator(0.1, 60, 1))
        self.step_delay_edit.setMinimumHeight(25)
        form_layout.addRow("升压延迟 (s):", self.step_delay_edit)

        # 循环时间
        self.cycle_time_edit = QLineEdit("10")
        self.cycle_time_edit.setValidator(QtGui.QDoubleValidator(1, 3600, 0))
        self.cycle_time_edit.setMinimumHeight(25)
        form_layout.addRow("循环时间 (s):", self.cycle_time_edit)

        layout.addLayout(form_layout)

        # 按钮布局
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

class CurrentStabilizationDialog(QDialog):
    """稳流参数设置对话框"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("稳流参数设置")
        self.setModal(True)
        self.setup_ui()

    def setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(10)
        layout.setContentsMargins(15, 15, 15, 15)

        # 创建表单布局
        form_layout = QFormLayout()
        form_layout.setVerticalSpacing(8)
        form_layout.setHorizontalSpacing(10)

        # 目标电流
        self.target_current_edit = QLineEdit("1000")
        self.target_current_edit.setValidator(QtGui.QDoubleValidator(1, 5000, 1))
        self.target_current_edit.setMinimumHeight(25)
        form_layout.addRow("目标电流 (uA):", self.target_current_edit)

        # 稳定范围
        self.stability_range_edit = QLineEdit("5")
        self.stability_range_edit.setValidator(QtGui.QDoubleValidator(0.5, 10, 1))
        self.stability_range_edit.setMinimumHeight(25)
        form_layout.addRow("稳定范围 (uA):", self.stability_range_edit)

        # 起始电压
        self.start_voltage_edit = QLineEdit("100")
        self.start_voltage_edit.setValidator(QtGui.QDoubleValidator(0, 5000, 1))
        self.start_voltage_edit.setMinimumHeight(25)
        form_layout.addRow("起始电压 (V):", self.start_voltage_edit)

        # 电流数据来源
        self.current_source_combo = QComboBox()
        self.current_source_combo.addItems(["Keithley自身", "阴极", "栅极", "阳极", "收集极"])
        self.current_source_combo.setMinimumHeight(25)
        form_layout.addRow("电流数据来源:", self.current_source_combo)

        # 稳流算法
        self.algorithm_combo = QComboBox()
        self.algorithm_combo.addItems(["PID", "接近算法(±范围内保持)"])
        self.algorithm_combo.setMinimumHeight(25)
        form_layout.addRow("稳流算法:", self.algorithm_combo)

        # 调整频率
        self.adjust_frequency_edit = QLineEdit("1")
        self.adjust_frequency_edit.setValidator(QtGui.QDoubleValidator(0.5, 5, 1))
        self.adjust_frequency_edit.setMinimumHeight(25)
        form_layout.addRow("调整频率 (s):", self.adjust_frequency_edit)

        # 最大调整电压
        self.max_adjust_voltage_edit = QLineEdit("50")
        self.max_adjust_voltage_edit.setValidator(QtGui.QDoubleValidator(1, 100, 1))
        self.max_adjust_voltage_edit.setMinimumHeight(25)
        form_layout.addRow("最大调整电压 (V):", self.max_adjust_voltage_edit)

        layout.addLayout(form_layout)

        # 按钮布局
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
        if len(s) == 4:  # #RGB -> #RRGGBB
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
        # 如果父窗口提供默认色表，优先使用；否则全部回退黑色
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

