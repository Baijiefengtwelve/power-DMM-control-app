from __future__ import annotations

from .common import *

from .utils import ScientificAxisItem

class ControlPanel(QWidget):
    """控制面板"""

    def __init__(self, main_window):
        super().__init__()
        self.main_window = main_window
        self.setup_ui()

    def setup_ui(self):
        # 创建滚动区域
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        scroll_area.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)

        # 创建内容部件
        content_widget = QWidget()
        scroll_area.setWidget(content_widget)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(scroll_area)

        # 内容布局
        content_layout = QVBoxLayout(content_widget)
        content_layout.setContentsMargins(8, 8, 8, 8)
        content_layout.setSpacing(8)

        title_label = QLabel("高压电源与万用表测试系统")
        title_label.setObjectName("titleLabel")
        title_label.setAlignment(Qt.AlignCenter)
        title_label.setMinimumHeight(35)
        content_layout.addWidget(title_label)

        hv_group = self.create_hv_group()
        content_layout.addWidget(hv_group)

        # 新增：Keithley 248高压源控制组
        keithley_group = self.create_keithley_group()
        content_layout.addWidget(keithley_group)

        meter_group = self.create_meter_group()
        content_layout.addWidget(meter_group)

        record_group = self.create_record_group()
        content_layout.addWidget(record_group)

        control_group = self.create_control_group()
        content_layout.addWidget(control_group)

        log_group = self.create_log_group()
        content_layout.addWidget(log_group)

        content_layout.addStretch()

    def create_hv_group(self):
        group = QGroupBox("高压电源控制")
        layout = QGridLayout(group)
        layout.setVerticalSpacing(6)
        layout.setHorizontalSpacing(6)
        layout.setContentsMargins(8, 15, 8, 8)

        layout.addWidget(QLabel("串口:"), 0, 0)
        self.main_window.hv_port_combo = QComboBox()
        self.main_window.hv_port_combo.setMinimumHeight(28)
        layout.addWidget(self.main_window.hv_port_combo, 0, 1)

        layout.addWidget(QLabel("波特率:"), 0, 2)
        self.main_window.hv_baudrate_combo = QComboBox()
        self.main_window.hv_baudrate_combo.setMinimumHeight(28)
        self.main_window.hv_baudrate_combo.addItems(["9600", "19200", "38400", "57600"])
        self.main_window.hv_baudrate_combo.setCurrentText("9600")
        layout.addWidget(self.main_window.hv_baudrate_combo, 0, 3)

        self.main_window.hv_refresh_btn = QPushButton("刷新")
        self.main_window.hv_refresh_btn.setMinimumSize(50, 28)
        self.main_window.hv_refresh_btn.clicked.connect(self.main_window.refresh_all_ports)
        layout.addWidget(self.main_window.hv_refresh_btn, 0, 4)

        self.main_window.hv_connect_btn = QPushButton("连接高压源")
        self.main_window.hv_connect_btn.setMinimumHeight(30)
        self.main_window.hv_connect_btn.clicked.connect(self.main_window.toggle_hv_connection)
        layout.addWidget(self.main_window.hv_connect_btn, 1, 0, 1, 5)

        layout.addWidget(QLabel("实际电压:"), 2, 0)
        self.main_window.hv_voltage_label = QLabel("未连接")
        self.main_window.hv_voltage_label.setObjectName("voltageLabel")
        self.main_window.hv_voltage_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(self.main_window.hv_voltage_label, 2, 1, 1, 4)

        # 手动设置电压输入框和按钮（新增部分）
        layout.addWidget(QLabel("手动设置电压(V):"), 3, 0)

        manual_set_layout = QHBoxLayout()
        self.main_window.manual_voltage_edit = QLineEdit("0")
        self.main_window.manual_voltage_edit.setValidator(QtGui.QDoubleValidator(0, 10000, 1))
        self.main_window.manual_voltage_edit.setMinimumHeight(28)
        self.main_window.manual_voltage_edit.setPlaceholderText("输入电压值 (0-10000V)")
        manual_set_layout.addWidget(self.main_window.manual_voltage_edit)

        self.main_window.manual_set_btn = QPushButton("设置")
        self.main_window.manual_set_btn.setMinimumHeight(28)
        self.main_window.manual_set_btn.setMinimumWidth(60)
        self.main_window.manual_set_btn.clicked.connect(self.main_window.manual_set_voltage)
        self.main_window.manual_set_btn.setEnabled(False)  # 初始时禁用，连接高压源后启用
        manual_set_layout.addWidget(self.main_window.manual_set_btn)

        layout.addLayout(manual_set_layout, 3, 1, 1, 4)

        # 测试设置按钮
        self.main_window.settings_btn = QPushButton("测试设置")
        self.main_window.settings_btn.setMinimumHeight(30)
        self.main_window.settings_btn.clicked.connect(self.main_window.show_test_settings)
        layout.addWidget(self.main_window.settings_btn, 4, 0, 1, 5)

        # 当前设置显示
        settings_layout = QVBoxLayout()
        settings_layout.setSpacing(4)

        self.main_window.current_settings_label = QLabel("当前设置: 未配置")
        self.main_window.current_settings_label.setWordWrap(True)
        self.main_window.current_settings_label.setObjectName("settingsLabel")
        self.main_window.current_settings_label.setMinimumHeight(40)
        settings_layout.addWidget(self.main_window.current_settings_label)

        layout.addLayout(settings_layout, 5, 0, 1, 5)

        button_layout1 = QHBoxLayout()
        self.main_window.start_test_btn = QPushButton("开始测试")
        self.main_window.start_test_btn.setMinimumHeight(30)
        self.main_window.start_test_btn.clicked.connect(self.main_window.start_test)
        self.main_window.start_test_btn.setEnabled(False)
        button_layout1.addWidget(self.main_window.start_test_btn)

        self.main_window.cycle_test_btn = QPushButton("循环测试")
        self.main_window.cycle_test_btn.setMinimumHeight(30)
        self.main_window.cycle_test_btn.clicked.connect(self.main_window.start_cycle_test)
        self.main_window.cycle_test_btn.setEnabled(False)
        button_layout1.addWidget(self.main_window.cycle_test_btn)

        layout.addLayout(button_layout1, 6, 0, 1, 5)

        button_layout2 = QHBoxLayout()
        self.main_window.stop_test_btn = QPushButton("停止测试")
        self.main_window.stop_test_btn.setMinimumHeight(30)
        self.main_window.stop_test_btn.clicked.connect(self.main_window.stop_test)
        self.main_window.stop_test_btn.setEnabled(False)
        button_layout2.addWidget(self.main_window.stop_test_btn)

        self.main_window.reset_btn = QPushButton("复位(100V)")
        self.main_window.reset_btn.setMinimumHeight(30)
        self.main_window.reset_btn.clicked.connect(self.main_window.reset_voltage)
        self.main_window.reset_btn.setEnabled(False)
        button_layout2.addWidget(self.main_window.reset_btn)

        layout.addLayout(button_layout2, 7, 0, 1, 5)

        return group

    def create_keithley_group(self):
        """创建Keithley 248高压源控制组"""
        group = QGroupBox("Keithley 248高压源")
        layout = QGridLayout(group)
        layout.setVerticalSpacing(6)
        layout.setHorizontalSpacing(6)
        layout.setContentsMargins(8, 15, 8, 8)

        layout.addWidget(QLabel("GPIB地址:"), 0, 0)
        self.main_window.keithley_addr_combo = QComboBox()
        self.main_window.keithley_addr_combo.setMinimumHeight(28)
        self.main_window.keithley_addr_combo.setEditable(True)  # 可编辑，允许输入地址
        layout.addWidget(self.main_window.keithley_addr_combo, 0, 1)

        # 说明：Keithley 248 的“刷新”按钮已与“高压电源控制”中的刷新整合
        self.main_window.keithley_connect_btn = QPushButton("连接")
        self.main_window.keithley_connect_btn.setMinimumSize(50, 28)
        self.main_window.keithley_connect_btn.clicked.connect(self.main_window.toggle_keithley_connection)
        layout.addWidget(self.main_window.keithley_connect_btn, 0, 2, 1, 2)

        # Keithley 电压显示（保留）
        layout.addWidget(QLabel("输出电压:"), 1, 0)
        self.main_window.keithley_voltage_label = QLabel("未连接")
        self.main_window.keithley_voltage_label.setObjectName("voltageLabel")
        self.main_window.keithley_voltage_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(self.main_window.keithley_voltage_label, 1, 1, 1, 3)

        # 删除“电源实际输出电流”显示标签（仍可在稳流中选择 Keithley 自身电流作为数据源）

        # 稳流设置按钮
        self.main_window.current_stabilization_btn = QPushButton("稳流设置")
        self.main_window.current_stabilization_btn.setMinimumHeight(30)
        self.main_window.current_stabilization_btn.clicked.connect(self.main_window.show_current_stabilization_settings)
        self.main_window.current_stabilization_btn.setEnabled(False)
        layout.addWidget(self.main_window.current_stabilization_btn, 2, 0, 1, 2)

        # 开始稳流按钮
        self.main_window.start_stabilization_btn = QPushButton("开始稳流")
        self.main_window.start_stabilization_btn.setMinimumHeight(30)
        self.main_window.start_stabilization_btn.clicked.connect(self.main_window.start_current_stabilization)
        self.main_window.start_stabilization_btn.setEnabled(False)
        layout.addWidget(self.main_window.start_stabilization_btn, 2, 2, 1, 2)

        # 停止稳流按钮
        self.main_window.stop_stabilization_btn = QPushButton("停止稳流")
        self.main_window.stop_stabilization_btn.setMinimumHeight(30)
        self.main_window.stop_stabilization_btn.clicked.connect(self.main_window.stop_current_stabilization)
        self.main_window.stop_stabilization_btn.setEnabled(False)
        layout.addWidget(self.main_window.stop_stabilization_btn, 3, 0, 1, 4)

        return group

    def create_meter_group(self):
        group = QGroupBox("万用表设置")
        layout = QGridLayout(group)
        layout.setVerticalSpacing(6)
        layout.setHorizontalSpacing(6)
        layout.setContentsMargins(8, 15, 8, 8)

        # 修改：增加备用万用表
        meter_configs = [
            ("阴极:", "cathode"),
            ("栅极:", "gate"),
            ("阳极:", "anode"),
            ("收集极:", "backup"),
            ("真空:", "vacuum")  # COMBIVAC CM52
        ]

        for i, (label, meter_type) in enumerate(meter_configs):
            layout.addWidget(QLabel(label), i, 0)

            port_combo = QComboBox()
            port_combo.setMinimumHeight(26)
            setattr(self.main_window, f"{meter_type}_port_combo", port_combo)
            layout.addWidget(port_combo, i, 1)

            coeff_edit = QLineEdit("1.0")
            coeff_edit.setMinimumHeight(26)
            coeff_edit.setMaximumWidth(60)
            setattr(self.main_window, f"{meter_type}_coeff", coeff_edit)

            coeff_layout = QHBoxLayout()
            coeff_layout.addWidget(QLabel("系数:"))
            coeff_layout.addWidget(coeff_edit)
            coeff_layout.setContentsMargins(0, 0, 0, 0)
            coeff_widget = QWidget()
            coeff_widget.setLayout(coeff_layout)
            layout.addWidget(coeff_widget, i, 2)

            connect_btn = QPushButton("连接")
            connect_btn.setMinimumSize(50, 26)
            connect_btn.clicked.connect(lambda checked, mt=meter_type: self.main_window.toggle_meter_connection(mt))
            setattr(self.main_window, f"{meter_type}_connect_btn", connect_btn)
            layout.addWidget(connect_btn, i, 3)

            value_label = QLabel("未连接")
            value_label.setObjectName("meterValue")
            value_label.setAlignment(Qt.AlignCenter)
            value_label.setMinimumHeight(26)
            setattr(self.main_window, f"{meter_type}_value_label", value_label)
            layout.addWidget(value_label, i, 4)

        return group

    def create_record_group(self):
        group = QGroupBox("数据记录")
        layout = QVBoxLayout(group)
        layout.setSpacing(6)
        layout.setContentsMargins(8, 15, 8, 8)

        path_layout = QHBoxLayout()
        self.main_window.path_btn = QPushButton("选择路径")
        self.main_window.path_btn.setMinimumHeight(28)
        self.main_window.path_btn.clicked.connect(self.main_window.select_path)
        path_layout.addWidget(self.main_window.path_btn)

        self.main_window.path_label = QLabel("未选择保存路径")
        self.main_window.path_label.setWordWrap(True)
        self.main_window.path_label.setObjectName("pathLabel")
        self.main_window.path_label.setMinimumHeight(40)
        path_layout.addWidget(self.main_window.path_label)
        layout.addLayout(path_layout)

        interval_layout = QHBoxLayout()
        interval_layout.addWidget(QLabel("保存间隔(s):"))
        self.main_window.interval_edit = QLineEdit("1")
        self.main_window.interval_edit.setMaximumWidth(50)
        self.main_window.interval_edit.setMinimumHeight(26)
        self.main_window.interval_edit.setValidator(QtGui.QIntValidator(1, 3600))
        interval_layout.addWidget(self.main_window.interval_edit)
        interval_layout.addStretch()
        layout.addLayout(interval_layout)

        self.main_window.record_btn = QPushButton("开始记录")
        self.main_window.record_btn.setMinimumHeight(30)
        self.main_window.record_btn.clicked.connect(self.main_window.toggle_record)
        self.main_window.record_btn.setEnabled(True)
        layout.addWidget(self.main_window.record_btn)

        # SQLite status + one-click cleanup (prevents data loss and keeps disk usage bounded)
        status_row = QHBoxLayout()
        self.main_window.db_status_label = QLabel("SQLite: -")
        self.main_window.db_status_label.setWordWrap(True)
        self.main_window.db_status_label.setMinimumHeight(22)
        status_row.addWidget(self.main_window.db_status_label)
        layout.addLayout(status_row)

        maint = QHBoxLayout()
        maint.addWidget(QLabel("保留天数:"))
        self.main_window.db_keep_days_edit = QLineEdit("30")
        self.main_window.db_keep_days_edit.setMaximumWidth(60)
        self.main_window.db_keep_days_edit.setValidator(QtGui.QIntValidator(1, 36500))
        maint.addWidget(self.main_window.db_keep_days_edit)
        maint.addWidget(QLabel("保留 runs:"))
        self.main_window.db_keep_runs_edit = QLineEdit("200")
        self.main_window.db_keep_runs_edit.setMaximumWidth(70)
        self.main_window.db_keep_runs_edit.setValidator(QtGui.QIntValidator(1, 1000000))
        maint.addWidget(self.main_window.db_keep_runs_edit)
        self.main_window.db_archive_chk = QCheckBox("清理前归档(CSV)")
        self.main_window.db_archive_chk.setChecked(True)
        maint.addWidget(self.main_window.db_archive_chk)
        maint.addWidget(QLabel("Vacuum:"))
        self.main_window.db_vacuum_mode_combo = QComboBox()
        self.main_window.db_vacuum_mode_combo.addItem("增量", "incremental")
        self.main_window.db_vacuum_mode_combo.addItem("全量", "vacuum")
        self.main_window.db_vacuum_mode_combo.setMaximumWidth(70)
        maint.addWidget(self.main_window.db_vacuum_mode_combo)
        maint.addStretch()
        layout.addLayout(maint)

        arch_row = QHBoxLayout()
        arch_row.addWidget(QLabel("归档目录:"))
        self.main_window.db_archive_dir_edit = QLineEdit(os.path.join('data', 'archive'))
        self.main_window.db_archive_dir_edit.setMinimumHeight(26)
        arch_row.addWidget(self.main_window.db_archive_dir_edit)
        self.main_window.db_cleanup_btn = QPushButton("一键清理数据库")
        self.main_window.db_cleanup_btn.setMinimumHeight(28)
        self.main_window.db_cleanup_btn.clicked.connect(self.main_window.on_db_cleanup_clicked)
        arch_row.addWidget(self.main_window.db_cleanup_btn)
        layout.addLayout(arch_row)

        # initialize label using current db stats (best-effort)
        try:
            self.main_window.update_db_status_label()
        except Exception:
            pass

        return group

    def create_control_group(self):
        group = QGroupBox("系统控制")
        layout = QHBoxLayout(group)
        layout.setContentsMargins(8, 15, 8, 8)

        self.main_window.clear_btn = QPushButton("清空图表")
        self.main_window.clear_btn.setMinimumHeight(30)
        self.main_window.clear_btn.clicked.connect(self.main_window.clear_plots)
        layout.addWidget(self.main_window.clear_btn)

        # 曲线颜色设置（UI可配置）
        self.main_window.plot_color_btn = QPushButton("曲线颜色设置")
        self.main_window.plot_color_btn.setMinimumHeight(30)
        self.main_window.plot_color_btn.clicked.connect(self.main_window.show_plot_color_settings)
        layout.addWidget(self.main_window.plot_color_btn)

        return group

    def create_log_group(self):
        group = QGroupBox("系统消息")
        layout = QVBoxLayout(group)
        layout.setContentsMargins(8, 15, 8, 8)
        self.main_window.log_text = QTextEdit()
        self.main_window.log_text.setReadOnly(True)
        self.main_window.log_text.setMinimumHeight(120)
        layout.addWidget(self.main_window.log_text)
        return group

class ChartPanel(QWidget):
    """图表面板"""

    def __init__(self, main_window):
        super().__init__()
        self.main_window = main_window
        self.setup_ui()

    def setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(6, 6, 6, 6)
        layout.setSpacing(6)

        chart_title = QLabel("实时数据监测")
        chart_title.setObjectName("chartTitle")
        chart_title.setAlignment(Qt.AlignCenter)
        chart_title.setMinimumHeight(30)
        layout.addWidget(chart_title)

        self.main_window.plot_widget = pg.PlotWidget(axisItems={'right': ScientificAxisItem(orientation='right')})
        self.main_window.plot_widget.setBackground('#FFFFFF')
        self.main_window.plot_widget.showGrid(x=True, y=True, alpha=0.3)
        self.main_window.plot_widget.setLabel('left', '数值', color='#2C3E50', size='10pt')
        self.main_window.plot_widget.setLabel('bottom', '时间 (s)', color='#2C3E50', size='10pt')

        self.main_window.plot_widget.getAxis('left').setPen(pg.mkPen(color='#2C3E50', width=1))
        self.main_window.plot_widget.getAxis('bottom').setPen(pg.mkPen(color='#2C3E50', width=1))

        # 绘图性能选项（不改变功能，只降低卡顿）
        self.main_window.plot_widget.setClipToView(True)
        self.main_window.plot_widget.setDownsampling(auto=True, mode='peak')
        # 兼容不同 pyqtgraph 版本：PlotWidget 本身没有 setAutoDownsample
        pi = self.main_window.plot_widget.getPlotItem()

        # 裁剪视窗外数据，显著减轻长时间运行卡顿
        try:
            pi.setClipToView(True)
        except Exception:
            pass

        # 自动下采样（不同版本签名不完全一致，做兼容）
        try:
            pi.setDownsampling(auto=True, mode="peak")
        except TypeError:
            try:
                pi.setDownsampling(auto=True)
            except Exception:
                pass
        except Exception:
            pass

        line_width = 1.5

        self.main_window.plot_widget.addLegend(offset=(5, 5), verSpacing=-5, labelTextSize='8pt')

        self.main_window.plots = {}

        # --- 右侧Y轴：真空度（独立量纲） ---
        try:
            self.main_window.plot_widget.showAxis('right')
            self.main_window.plot_widget.setLabel('right', '真空', units='Pa', color='#2C3E50', size='10pt')
            try:
                self.main_window.plot_widget.getAxis('right').enableAutoSIPrefix(False)
            except Exception:
                pass
            self.main_window._vacuum_vb = pg.ViewBox()
            self.main_window.plot_widget.scene().addItem(self.main_window._vacuum_vb)
            self.main_window._vacuum_vb.setXLink(self.main_window.plot_widget.getViewBox())
            self.main_window.plot_widget.getAxis('right').linkToView(self.main_window._vacuum_vb)

            def _update_views():
                self.main_window._vacuum_vb.setGeometry(self.main_window.plot_widget.getViewBox().sceneBoundingRect())
                self.main_window._vacuum_vb.linkedViewChanged(self.main_window.plot_widget.getViewBox(), self.main_window._vacuum_vb.XAxis)

            self.main_window.plot_widget.getViewBox().sigResized.connect(_update_views)
            _update_views()
        except Exception:
            pass


        def _pen(key, fallback):
            return pg.mkPen(color=self.main_window.get_plot_color(key, fallback), width=line_width)

        self.main_window.plots['cathode'] = self.main_window.plot_widget.plot(
            pen=_pen('cathode', '#E74C3C'),
            name='阴极'
        )
        self.main_window.plots['gate'] = self.main_window.plot_widget.plot(
            pen=_pen('gate', '#2ECC71'),
            name='栅极'
        )
        self.main_window.plots['anode'] = self.main_window.plot_widget.plot(
            pen=_pen('anode', '#3498DB'),
            name='阳极'
        )
        self.main_window.plots['backup'] = self.main_window.plot_widget.plot(
            pen=_pen('backup', '#F39C12'),
            name='收集极'
        )
        self.main_window.plots['keithley_voltage'] = self.main_window.plot_widget.plot(
            pen=_pen('keithley_voltage', '#9B59B6'),
            name='栅极电压'
        )
        self.main_window.plots['gate_plus_anode'] = self.main_window.plot_widget.plot(
            pen=_pen('gate_plus_anode', '#E67E22'),
            name='栅极+阳极+收集极'
        )
        self.main_window.plots['anode_cathode_ratio'] = self.main_window.plot_widget.plot(
            pen=_pen('anode_cathode_ratio', '#1ABC9C'),
            name='(阳极/阴极)×100'
        )

        # 真空曲线走右侧Y轴
        try:
            vac_item = pg.PlotDataItem(
                pen=_pen('vacuum', '#7F8C8D'),
                name='真空'
            )
            if hasattr(self.main_window, "_vacuum_vb"):
                self.main_window._vacuum_vb.addItem(vac_item)
            else:
                self.main_window.plot_widget.addItem(vac_item)
            self.main_window.plots['vacuum'] = vac_item
            # 添加到图例
            try:
                if self.main_window.plot_widget.legend is not None:
                    self.main_window.plot_widget.legend.addItem(vac_item, '真空')
            except Exception:
                pass
        except Exception:
            pass


        # 启动时按配置覆盖默认曲线颜色
        try:
            if hasattr(self.main_window, "apply_plot_colors"):
                self.main_window.apply_plot_colors()
        except Exception:
            pass

        layout.addWidget(self.main_window.plot_widget)
