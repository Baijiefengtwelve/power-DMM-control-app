from __future__ import annotations

from .common import *

from .utils import ScientificAxisItem
from .config_manager import ConfigManager
from .controllers import Keithley248Controller, HAPS06Controller
from .threads import HVVoltagePoller, HVConnectThread, PIDController, CurrentStabilizationThread, SerialThread, CM52Thread, CountdownManager, DataSaver
from .ui_dialogs import TestSettingsDialog, CurrentStabilizationDialog, PlotColorDialog
from .ui_panels import ControlPanel, ChartPanel
from .data_buffer import DataBuffer
from .services import TestService, StabilizationService
from .monitoring.influx_writer import InfluxWriter
from .sqlite_recorder import SQLiteRecorder
from .sqlite_maintenance import load_retention_from_config, db_stats, cleanup_db

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        # Tray-mode exit control: by default, closing the window hides it.
        # Use request_quit() to perform a full shutdown.
        self._allow_quit = False
        self.tray_icon = None  # populated by launcher if tray is enabled
        self.config_manager = ConfigManager()
        self.config = self.config_manager.load_config()

        # Optional: write time-series data to InfluxDB for dashboards/diagnostics
        self.influx_writer = InfluxWriter.from_config(self.config)

        # Crash-safe local persistence (SQLite) + retention
        self.sqlite_recorder = SQLiteRecorder.from_config(self.config)
        self.retention_policy = load_retention_from_config(self.config)

        # Monitoring context
        self.session_id = time.strftime('%Y%m%d_%H%M%S')
        self.current_run_id = ''
        self._prev_testing = False
        self._prev_stabilizing = False
        self._prev_recording = False

        self._keithley_v_cache = 0.0
        self._keithley_v_ts = 0.0
        # 测试参数
        self.test_params = {
            'start_voltage': 0,
            'target_voltage': 1000,
            'voltage_step': 10,
            'step_delay': 1,
            'cycle_time': 10
        }

        # 稳流参数
        self.stabilization_params = {
            'target_current': 1000,  # uA
            'stability_range': 5,  # uA
            'start_voltage': 100,  # V
            'current_source': 'keithley',  # 'keithley', 'cathode', 'gate', 'anode', 'backup'
            'adjust_frequency': 1,  # s
            'max_adjust_voltage': 50,  # V
            'algorithm': 'pid'  # 'pid' or 'approach'
        }

        self.setup_ui()
        self.setup_controllers()
        try:
            self.influx_writer.start()
        except Exception:
            pass
        try:
            self.sqlite_recorder.start()
        except Exception:
            pass
        self._setup_services()
        self.setup_timers()
        self.refresh_all_ports()
        self.load_config_to_ui()
        self.update_settings_display()

    def request_quit(self):
        """Request a full application quit (used by tray menu).

        In tray mode, closing the window does not terminate the process because
        QApplication.setQuitOnLastWindowClosed(False) is enabled. Therefore we
        must explicitly call QApplication.quit() to trigger aboutToQuit and
        allow the launcher to stop Web/InfluxDB.
        """
        try:
            self._allow_quit = True
        except Exception:
            pass

        # Trigger normal Qt shutdown path (runs closeEvent cleanup).
        try:
            self.close()
        except Exception:
            pass

        # Ensure the Qt event loop exits (emits aboutToQuit).
        try:
            app = QApplication.instance()
            if app is not None:
                app.quit()
        except Exception:
            pass

    def _setup_services(self):
        """Init business services and connect signals."""
        self.test_service = TestService(self)
        self.test_service.log.connect(self.log_message)
        self.test_service.state_change.connect(self._on_test_state_change)
        self.test_service.finished.connect(self._on_test_finished)

        self.stabilization_service = StabilizationService(self)
        self.stabilization_service.log.connect(self.log_message)
    def _on_test_state_change(self, state: dict):
        """Sync UI buttons with test state changes (and handle countdown)."""
        try:
            # Remember last test type (used by _on_test_finished)
            try:
                if "cycle" in state:
                    self._last_test_was_cycle = bool(state.get("cycle"))
            except Exception:
                pass

            # Countdown control (cycle wait)
            try:
                if state.get("countdown_stop"):
                    self.countdown_manager.stop()
                    self.update_countdown_display(0)
                if "countdown_start" in state:
                    seconds = int(state.get("countdown_start") or 0)
                    if seconds > 0:
                        self.countdown_manager.start(seconds)
                    else:
                        self.countdown_manager.stop()
                        self.update_countdown_display(0)
            except Exception:
                pass
        except Exception:
            pass

        testing = bool(state.get("testing", False))

        # Track transitions (lightweight monitoring)
        try:
            if testing != getattr(self, "_prev_testing", False):
                self._prev_testing = testing
        except Exception:
            pass
        try:
            if bool(getattr(self, "is_stabilizing", False)) != getattr(self, "_prev_stabilizing", False):
                self._prev_stabilizing = bool(getattr(self, "is_stabilizing", False))
        except Exception:
            pass
        try:
            if bool(getattr(self, "is_recording", False)) != getattr(self, "_prev_recording", False):
                self._prev_recording = bool(getattr(self, "is_recording", False))
        except Exception:
            pass

        # Button enable/disable
        try:
            self.start_test_btn.setEnabled(not testing)
            self.cycle_test_btn.setEnabled(not testing)
            self.stop_test_btn.setEnabled(testing)
            self.manual_set_btn.setEnabled(not testing)
        except Exception:
            pass

    def _on_test_finished(self):
        """Test finished: stop auto-recording and compute single-test minima."""
        try:
            was_cycle = bool(getattr(self, "_last_test_was_cycle", getattr(self, "is_cycle_testing", False)))

            # Stop countdown display when a test ends
            try:
                self.countdown_manager.stop()
                self.update_countdown_display(0)
            except Exception:
                pass

            # Stop auto-recording (set flag first so conversion can compute minima)
            if getattr(self, "auto_recording", False) and self.is_recording:
                self.auto_recording = False
                self.toggle_record()
                self.log_message("测试结束：已自动停止记录")

            # Single test: log min (I, V, time) for convenience
            if not was_cycle:
                try:
                    min_a = None
                    min_v = None
                    min_t = None
                    if getattr(self, "anode_min_value", None) is not None:
                        min_a = self.anode_min_value
                        min_item = next(item for item in self.all_anode_data if item[0] == min_a)
                        min_v, min_t = min_item[1], min_item[2]
                    if min_a is not None:
                        self.log_message(
                            f"单次测试最小阳极电流: {min_a:.6g}  对应电压: {float(min_v):.1f}V  时间: {min_t}"
                        )
                except Exception as e:
                    self.log_message(f"计算单次最小值失败: {e}")
        except Exception:
            pass


    def setup_ui(self):
        self.setWindowTitle("高压电源与万用表测试系统")
        self.setGeometry(20, 20, 1260, 760)

        self.setStyleSheet("""
            QMainWindow {
                background-color: #F5F7FA;
            }
            QWidget {
                font-family: 'Microsoft YaHei', sans-serif;
                font-size: 9pt;
            }
            QGroupBox {
                font-weight: bold;
                border: 1px solid #D1D9E6;
                border-radius: 5px;
                margin-top: 0.5ex;
                padding-top: 8px;
                background-color: #FFFFFF;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                subcontrol-position: top center;
                padding: 2px 8px;
                background-color: #4A6572;
                color: #FFFFFF;
                border-radius: 3px;
                font-size: 9pt;
            }
            QPushButton {
                background-color: #5D7B9D;
                color: white;
                border: none;
                border-radius: 3px;
                padding: 4px 8px;
                min-height: 22px;
                min-width: 60px;
                font-weight: bold;
                font-size: 9pt;
            }
            QPushButton:hover {
                background-color: #4A6572;
            }
            QPushButton:pressed {
                background-color: #344955;
            }
            QPushButton:disabled {
                background-color: #B0BEC5;
                color: #757575;
            }
            QLineEdit, QComboBox {
                padding: 2px 6px;
                border: 1px solid #D1D9E6;
                border-radius: 3px;
                background-color: white;
                font-size: 9pt;
                min-height: 22px;
            }
            QTextEdit {
                border: 1px solid #D1D9E6;
                border-radius: 3px;
                background-color: white;
                font-family: Consolas, monospace;
                font-size: 8pt;
            }
            QLabel#titleLabel {
                font-size: 12pt;
                font-weight: bold;
                padding: 6px;
                background-color: #4A6572;
                color: white;
                border-radius: 3px;
                text-align: center;
            }
            QLabel#chartTitle {
                font-size: 11pt;
                font-weight: bold;
                padding: 5px;
                background-color: #4A6572;
                color: white;
                border-radius: 3px;
                text-align: center;
            }
            QLabel#voltageLabel {
                font-size: 11pt;
                font-weight: bold;
                color: #D32F2F;
                padding: 3px;
                background-color: #FFEBEE;
                border: 1px solid #EF5350;
                border-radius: 3px;
            }
            QLabel#meterValue {
                font-size: 8pt;
                padding: 2px 4px;
                background-color: #E8F5E8;
                border: 1px solid #81C784;
                border-radius: 3px;
                min-width: 70px;
                font-weight: bold;
                color: #2E7D32;
            }
            QLabel#pathLabel {
                background-color: #FFF3E0;
                padding: 3px;
                font-size: 8pt;
                border: 1px solid #FFB74D;
                border-radius: 3px;
            }
            QLabel#countdown {
                font-size: 10pt;
                font-weight: bold;
                color: #D32F2F;
                padding: 3px 5px;
                background-color: #FFEBEE;
                border: 1px solid #EF5350;
                border-radius: 3px;
            }
            QLabel#settingsLabel {
                background-color: #E3F2FD;
                padding: 3px;
                font-size: 8pt;
                border: 1px solid #64B5F6;
                border-radius: 3px;
            }
            QStatusBar {
                background-color: #E1E8ED;
                color: #2C3E50;
                font-size: 9pt;
            }
            QScrollArea {
                border: none;
                background-color: transparent;
            }
        """)

        main_widget = QWidget()
        self.setCentralWidget(main_widget)

        splitter = QSplitter(Qt.Horizontal)
        splitter.setHandleWidth(2)

        self.control_panel = ControlPanel(self)
        splitter.addWidget(self.control_panel)

        self.chart_panel = ChartPanel(self)
        splitter.addWidget(self.chart_panel)

        # 调整分割比例，使控制面板更紧凑
        splitter.setSizes([480, 900])
        splitter.setStretchFactor(0, 0)
        splitter.setStretchFactor(1, 1)

        splitter.setMinimumSize(1380, 820)

        splitter.setStyleSheet("""
            QSplitter::handle {
                background-color: #D1D9E6;
                width: 2px;
            }
            QSplitter::handle:hover {
                background-color: #5D7B9D;
            }
        """)

        main_layout = QHBoxLayout(main_widget)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.addWidget(splitter)

        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)

        self.countdown_label = QLabel("")
        self.countdown_label.setObjectName("countdown")
        self.status_bar.addPermanentWidget(self.countdown_label)

        self.status_bar.showMessage("系统就绪 - 请连接设备开始测试")

    # -------- 曲线颜色（UI可配置）--------
    def _default_plot_colors(self):
        return {
            'cathode': '#E74C3C',
            'gate': '#2ECC71',
            'anode': '#3498DB',
            'backup': '#F39C12',
            'keithley_voltage': '#9B59B6',
            'gate_plus_anode': '#E67E22',
            'anode_cathode_ratio': '#1ABC9C',
            'vacuum': '#7F8C8D'
        }

    def get_plot_color(self, key, fallback='#000000'):
        try:
            if self.config and self.config.has_section('PlotColors'):
                v = self.config.get('PlotColors', key, fallback=fallback)
                if v:
                    return str(v).strip()
        except Exception:
            pass
        return fallback

    def _save_plot_colors_to_config(self, colors_dict):
        try:
            cfg = {}
            # 保留原有配置，新增/覆盖 PlotColors
            cfg['PlotColors'] = {}
            for k, v in colors_dict.items():
                cfg['PlotColors'][k] = v
            self.config_manager.save_config(cfg)
            # 重新加载到内存
            self.config = self.config_manager.load_config()
            return True
        except Exception as e:
            self.log_message(f"保存曲线颜色失败: {e}")
            return False

    def apply_plot_colors(self):
        """将配置颜色应用到已创建的曲线"""
        if not hasattr(self, 'plots') or not isinstance(self.plots, dict):
            return
        defaults = self._default_plot_colors()
        for key, item in self.plots.items():
            try:
                color = self.get_plot_color(key, defaults.get(key, '#000000'))
                item.setPen(pg.mkPen(color=color, width=1.5))
            except Exception:
                pass

    def show_plot_color_settings(self):
        """打开曲线颜色设置对话框"""
        try:
            series = [
                ('cathode', '阴极'),
                ('gate', '栅极'),
                ('anode', '阳极'),
                ('backup', '收集极'),
                ('keithley_voltage', '栅极电压'),
                ('gate_plus_anode', '栅极+阳极+收集极'),
                ('anode_cathode_ratio', '(阳极/阴极)×100'),
                ('vacuum', '真空'),
            ]
            defaults = self._default_plot_colors()
            current = {k: self.get_plot_color(k, defaults.get(k, '#000000')) for k, _ in series}

            dlg = PlotColorDialog(self, series=series, current_colors=current)
            if dlg.exec_() == QDialog.Accepted:
                new_colors = dlg.get_colors()
                # 用默认值补齐被“重置”的项
                for k, _ in series:
                    if k not in new_colors:
                        new_colors[k] = defaults.get(k, current.get(k, '#000000'))
                if self._save_plot_colors_to_config(new_colors):
                    self.apply_plot_colors()
                    self.log_message("曲线颜色已保存并应用。")
        except Exception as e:
            self.log_message(f"打开曲线颜色设置失败: {e}")

    def setup_controllers(self):
        """初始化控制器和数据"""
        self.hv_controller = HAPS06Controller()
        # 注意：不要直接用 controller 回调更新UI（可能在后台线程里触发，导致UI不刷新/卡顿/崩溃）
        self.hv_controller.voltage_update_callback = None

        # HAPS06 实际电压后台轮询（实时刷新标签 & 写入数据）
        self.hv_voltage_poller = None
        self._hv_v_cache = 0.0
        self._hv_v_ts = 0.0


        # 新增：Keithley 248控制器
        self.keithley_controller = Keithley248Controller()

        self.meter_threads = {}
        self.meter_data = {
            'cathode': {'value': 0, 'unit': '', 'coefficient': 1.0, 'timestamp': 0.0, 'valid': False},
            'gate': {'value': 0, 'unit': '', 'coefficient': 1.0, 'timestamp': 0.0, 'valid': False},
            'anode': {'value': 0, 'unit': '', 'coefficient': 1.0, 'timestamp': 0.0, 'valid': False},
            'backup': {'value': 0, 'unit': '', 'coefficient': 1.0, 'timestamp': 0.0, 'valid': False}
        ,
            'vacuum': {'value': 0, 'unit': 'Pa', 'coefficient': 1.0, 'timestamp': 0.0, 'valid': False}
        }
        self.data_mutex = QMutex()

        # 使用优化的数据缓冲区
        self.data_buffer = DataBuffer(max_points=3000)

        # 后台数据保存线程
        self.data_saver = DataSaver()
        self.data_saver.save_complete.connect(self.on_data_saved)
        self.data_saver.convert_complete.connect(self.on_data_converted)
        self.data_saver.start()

        self.cycle_data = []
        self.current_cycle_anode_data = []

        # 优化内存使用
        self.recorded_data = deque(maxlen=10000)  # 限制最大记录数
        self.all_anode_data = deque(maxlen=10000)
        # 长测优化：用运行最小值记录阳极最小值，避免 deque 截断导致统计不准，也避免停止时扫描大列表
        self.anode_min_value = None
        self.anode_min_voltage = None
        self.anode_min_time = None

        # 添加批量数据缓存
        self.data_cache = []
        self.cache_size = 50  # 增加缓存大小，减少写入频率
        # 小批量也要定期入队，避免 batch 未触发导致长时间不落盘
        self.cache_send_interval = 1.0  # seconds
        self._last_cache_send_ts = 0.0

        self.save_timer = QTimer()
        self.save_timer.timeout.connect(self.save_data)
        self.excel_file = None
        self.is_recording = False

        self.is_converting = False  # 后台转换Excel中
        self.is_testing = False
        self.is_cycle_testing = False
        self.current_cycle = 0
        self.test_mode = "升压"

        # 自动记录标志
        self.auto_recording = False
        # 循环测试记录状态标志
        self.cycle_recording_active = False

        # 新增：稳流控制相关
        self.is_stabilizing = False
        self.stabilization_thread = None

        self.countdown_manager = CountdownManager(self.update_countdown_display)

        # 优化：添加数据更新队列
        self.data_update_queue = []
        self.last_meter_update_time = 0
        self.meter_update_interval = 0.5  # 万用表标签更新间隔100ms

    def setup_timers(self):
        """初始化定时器"""
        self.hv_voltage_update_timer = QTimer()
        self.hv_voltage_update_timer.timeout.connect(self.update_hv_voltage)
        self.hv_voltage_update_timer.start(1000)  # 高压源电压更新500ms

        # 新增：Keithley 248电压更新定时器
        self.keithley_voltage_update_timer = QTimer()
        self.keithley_voltage_update_timer.timeout.connect(self.update_keithley_voltage)
        self.keithley_voltage_update_timer.start(1000)  # 每秒更新一次

        # 优化：降低图表更新频率，提高性能
        self.data_update_timer = QTimer()
        self.data_update_timer.timeout.connect(self.update_plots)
        self.data_update_timer.start(1000)  # 图表更新200ms

        self.status_update_timer = QTimer()
        self.status_update_timer.timeout.connect(self.update_status_display)
        self.status_update_timer.start(1000)  # 状态更新1s

        # 优化：添加万用表标签更新定时器
        self.meter_display_timer = QTimer()
        self.meter_display_timer.timeout.connect(self.update_meter_displays)
        self.meter_display_timer.start(500)  # 万用表标签更新100ms

        # 添加数据缓存定时器
        self.cache_flush_timer = QTimer()
        self.cache_flush_timer.timeout.connect(self.flush_data_cache)
        self.cache_flush_timer.start(5000)  # 每5秒强制刷新一次缓存

    def refresh_gpib_ports(self):
        """刷新GPIB端口（仅添加实际扫描到的GPIB资源，不再填充默认地址）"""
        try:
            # Keithley已连接时，地址下拉框被锁定；此时不刷新列表，避免改变当前选择
            if not self.keithley_addr_combo.isEnabled():
                self.log_message("Keithley已连接，跳过GPIB列表刷新")
                return
            try:
                import pyvisa
                rm = pyvisa.ResourceManager()
                resources = rm.list_resources()

                # 清空下拉框（只保留扫描结果）
                self.keithley_addr_combo.clear()

                gpib_devices = []
                for resource in resources:
                    if 'GPIB' in resource or 'gpib' in resource.lower():
                        gpib_devices.append(resource)

                # 只把扫描到的资源加入下拉框
                if gpib_devices:
                    for r in gpib_devices:
                        self.keithley_addr_combo.addItem(r)
                    self.log_message(f"找到 {len(gpib_devices)} 个GPIB资源")
                else:
                    # 不再填充默认地址；下拉框保持为空，但可手动输入
                    self.log_message("未找到GPIB资源（下拉框保持空，可手动输入地址）")

            except ImportError:
                self.log_message("未安装pyvisa库，请使用 'pip install pyvisa' 安装（下拉框保持空，可手动输入地址）")
                self.keithley_addr_combo.clear()

        except Exception as e:
            error_msg = f"刷新GPIB端口错误: {str(e)}"
            self.log_message(error_msg)
            # 出错时也不再填充默认地址；保持为空
            self.keithley_addr_combo.clear()

    def _start_hv_connection_async(self, port: str, baudrate: int):
        """异步连接HAPS06，避免UI线程被超时阻塞。"""
        try:
            th = getattr(self, "_hv_connect_thread", None)
            if th is not None and getattr(th, "isRunning", lambda: False)():
                self.log_message("高压源正在连接中，请稍候...")
                return
        except Exception:
            pass

        # UI lock during connect
        try:
            self.hv_connect_btn.setEnabled(False)
            self.hv_connect_btn.setText("连接中...")
        except Exception:
            pass
        try:
            self.hv_port_combo.setEnabled(False)
            self.hv_baudrate_combo.setEnabled(False)
        except Exception:
            pass
        try:
            self.hv_refresh_btn.setEnabled(False)
        except Exception:
            pass

        # Prevent starting tests while connecting
        try:
            self.start_test_btn.setEnabled(False)
            self.cycle_test_btn.setEnabled(False)
            self.reset_btn.setEnabled(False)
            self.manual_set_btn.setEnabled(False)
        except Exception:
            pass

        try:
            self.status_bar.showMessage("高压源连接中...")
        except Exception:
            pass

        self._hv_connect_thread = HVConnectThread(self.hv_controller, port, baudrate, remote_timeout_s=1.5)
        try:
            self._hv_connect_thread.progress.connect(lambda m: self.log_message(f"[HAPS06] {m}"))
        except Exception:
            try:
                self._hv_connect_thread.progress.connect(self.log_message)
            except Exception:
                pass
        try:
            self._hv_connect_thread.finished.connect(self._on_hv_connect_finished)
        except Exception:
            pass
        self._hv_connect_thread.start()

    def _on_hv_connect_finished(self, success: bool, message: str, port: str):
        """HAPS06异步连接结果处理"""
        try:
            self.hv_connect_btn.setEnabled(True)
        except Exception:
            pass

        if success:
            try:
                self.hv_connect_btn.setText("断开高压源")
            except Exception:
                pass
            # Lock port/baud combos, but keep refresh disabled
            try:
                self.hv_port_combo.setEnabled(False)
                self.hv_baudrate_combo.setEnabled(False)
            except Exception:
                pass
            try:
                self.hv_refresh_btn.setEnabled(False)
            except Exception:
                pass

            try:
                self.start_test_btn.setEnabled(True)
                self.cycle_test_btn.setEnabled(True)
                self.reset_btn.setEnabled(True)
                self.manual_set_btn.setEnabled(True)
            except Exception:
                pass

            self.log_message(f"高压源已连接到: {port}")
            self.log_message(f"{message}")
            try:
                self._attach_hv_worker_signals()
            except Exception:
                pass
            try:
                self.status_bar.showMessage(f"高压源已连接 - {port}")
            except Exception:
                pass
            # Start poller for actual voltage
            try:
                self.start_hv_voltage_poller(interval_ms=800)
            except Exception:
                pass
        else:
            # Ensure disconnected state
            try:
                self._detach_hv_worker_signals()
            except Exception:
                pass
            try:
                self.hv_controller.disconnect()
            except Exception:
                pass

            try:
                self.hv_port_combo.setEnabled(True)
                self.hv_baudrate_combo.setEnabled(True)
            except Exception:
                pass
            try:
                self.hv_refresh_btn.setEnabled(True)
            except Exception:
                pass
            try:
                self.hv_connect_btn.setText("连接高压源")
            except Exception:
                pass
            try:
                self.hv_voltage_label.setText("未连接")
                self.hv_voltage_label.setStyleSheet("font-size: 11pt; font-weight: bold; color: #D32F2F; padding: 3px;")
            except Exception:
                pass

            self.log_message(f"高压源连接失败: {message}")
            try:
                self.status_bar.showMessage("高压源连接失败")
            except Exception:
                pass



    def toggle_keithley_connection(self):
        """连接/断开Keithley 248高压源"""
        try:
            if self.keithley_connect_btn.text() == "连接":
                resource_name = self.keithley_addr_combo.currentText()

                if not resource_name:
                    self.log_message("错误: 请选择或输入GPIB地址")
                    return

                self.log_message(f"正在连接Keithley 248, GPIB地址: {resource_name}...")

                # 从资源名中提取地址
                address = resource_name
                if "::" in resource_name:
                    # 格式为 GPIB0::14::INSTR，提取中间的地址
                    parts = resource_name.split("::")
                    if len(parts) >= 2:
                        address = parts[1]

                success, message = self.keithley_controller.connect_gpib(address)

                if success:
                    self.keithley_connect_btn.setText("断开")

                    # 连接后锁定GPIB地址下拉框，避免更改

                    self.keithley_addr_combo.setEnabled(False)
                    self.current_stabilization_btn.setEnabled(True)
                    self.start_stabilization_btn.setEnabled(True)
                    self.log_message(f"Keithley 248已连接: {message}")

                    # 更新显示
                    self.update_keithley_voltage()

                    self.status_bar.showMessage(f"Keithley 248已连接 - {resource_name}")

                else:
                    self.log_message(f"Keithley 248连接失败: {message}")
                    self.status_bar.showMessage("Keithley 248连接失败")

            else:
                # 断开连接
                if self.is_stabilizing:
                    self.stop_current_stabilization()

                self.keithley_controller.disconnect()
                # 断开后允许重新选择GPIB地址
                self.keithley_addr_combo.setEnabled(True)
                self.keithley_connect_btn.setText("连接")
                self.current_stabilization_btn.setEnabled(False)
                self.start_stabilization_btn.setEnabled(False)
                self.stop_stabilization_btn.setEnabled(False)
                self.keithley_voltage_label.setText("未连接")
                self.log_message("Keithley 248已断开")
                self.status_bar.showMessage("Keithley 248已断开")

        except Exception as e:
            error_msg = f"Keithley 248连接/断开错误: {str(e)}"
            self.log_message(error_msg)

    def update_keithley_voltage(self):
        """更新Keithley 248电压显示（已移除电流标签显示）"""
        try:
            if self.keithley_controller.is_connected:
                voltage = self.keithley_controller.read_voltage()

                if voltage is not None:
                    self.keithley_voltage_label.setText(f"{voltage:.1f} V")
                    self.keithley_voltage_label.setStyleSheet(
                        "font-size: 11pt; font-weight: bold; color: #2E7D32; padding: 3px;")
                else:
                    self.keithley_voltage_label.setText("读取失败")
                    self.keithley_voltage_label.setStyleSheet(
                        "font-size: 11pt; font-weight: bold; color: #D32F2F; padding: 3px;")

        except Exception as e:
            error_msg = f"更新Keithley电压错误: {str(e)}"
            self.log_message(error_msg)

    def show_current_stabilization_settings(self):
        """显示稳流参数设置对话框"""
        dialog = CurrentStabilizationDialog(self)

        # 设置当前值
        dialog.target_current_edit.setText(str(self.stabilization_params['target_current']))
        dialog.stability_range_edit.setText(str(self.stabilization_params['stability_range']))
        dialog.start_voltage_edit.setText(str(self.stabilization_params['start_voltage']))

        # 设置电流数据来源
        source_index = 0
        if self.stabilization_params['current_source'] == 'cathode':
            source_index = 1
        elif self.stabilization_params['current_source'] == 'gate':
            source_index = 2
        elif self.stabilization_params['current_source'] == 'anode':
            source_index = 3
        elif self.stabilization_params['current_source'] == 'backup':
            source_index = 4
        dialog.current_source_combo.setCurrentIndex(source_index)

        dialog.adjust_frequency_edit.setText(str(self.stabilization_params['adjust_frequency']))
        dialog.max_adjust_voltage_edit.setText(str(self.stabilization_params['max_adjust_voltage']))

        # 稳流算法
        algo = str(self.stabilization_params.get('algorithm', 'pid')).lower()
        if hasattr(dialog, 'algorithm_combo'):
            dialog.algorithm_combo.setCurrentIndex(0 if algo != 'approach' else 1)

        if dialog.exec_() == QDialog.Accepted:
            # 保存设置
            try:
                self.stabilization_params['target_current'] = float(dialog.target_current_edit.text())
                self.stabilization_params['stability_range'] = float(dialog.stability_range_edit.text())
                self.stabilization_params['start_voltage'] = float(dialog.start_voltage_edit.text())

                # 获取电流数据来源
                source_index = dialog.current_source_combo.currentIndex()
                if source_index == 0:
                    self.stabilization_params['current_source'] = 'keithley'
                elif source_index == 1:
                    self.stabilization_params['current_source'] = 'cathode'
                elif source_index == 2:
                    self.stabilization_params['current_source'] = 'gate'
                elif source_index == 3:
                    self.stabilization_params['current_source'] = 'anode'
                elif source_index == 4:
                    self.stabilization_params['current_source'] = 'backup'

                self.stabilization_params['adjust_frequency'] = float(dialog.adjust_frequency_edit.text())
                self.stabilization_params['max_adjust_voltage'] = float(dialog.max_adjust_voltage_edit.text())

                # 保存稳流算法
                if hasattr(dialog, 'algorithm_combo'):
                    self.stabilization_params['algorithm'] = 'pid' if dialog.algorithm_combo.currentIndex() == 0 else 'approach'

                self.log_message("稳流参数已更新")
                self.save_config_from_ui()

            except ValueError as e:
                self.log_message(f"稳流参数设置错误: {str(e)}")

    def start_current_stabilization(self):
        """开始稳流控制（服务包装）"""
        try:
            return self.stabilization_service.start()
        except Exception:
            return self._start_current_stabilization_impl()

    def _start_current_stabilization_impl(self):
        """Original stabilization start implementation."""
        if not self.keithley_controller.is_connected:
            self.log_message("错误: Keithley 248未连接")
            return

        if self.is_stabilizing:
            self.log_message("稳流控制已在运行")
            return

        self.log_message("开始稳流控制...")

        # 创建稳流控制线程
        self.stabilization_thread = CurrentStabilizationThread(
            self.keithley_controller,
            self.meter_data,
            self.data_mutex,
            self.stabilization_params
        )

        self.stabilization_thread.update_voltage_signal.connect(self.on_keithley_voltage_updated)
        self.stabilization_thread.update_status_signal.connect(self.log_message)
        self.stabilization_thread.stabilization_complete_signal.connect(self.on_stabilization_complete)

        self.is_stabilizing = True
        self.start_stabilization_btn.setEnabled(False)
        self.stop_stabilization_btn.setEnabled(True)
        self.current_stabilization_btn.setEnabled(False)

        self.stabilization_thread.start()

    def stop_current_stabilization(self):
        """停止稳流控制（手动停止；停止后置零并关高压，同时界面归零）"""
        if not self.is_stabilizing:
            return

        self.log_message("停止稳流控制...")

        # 先让稳流线程退出（线程内部也会 best-effort 置零并关高压）
        if self.stabilization_thread:
            try:
                self.stabilization_thread.stop()
            except Exception:
                pass
            self.stabilization_thread.wait(3000)
            self.stabilization_thread = None

        # 再做一次双保险：VSET 0 + HVOF
        if self.keithley_controller.is_connected:
            try:
                self.keithley_controller.set_voltage(0.0)
            except Exception as e:
                self.log_message(f"置零电压失败: {e}")

            try:
                self.keithley_controller.disable_high_voltage()
                self.log_message("已关闭248高压输出 (HVOF)")
            except Exception as e:
                self.log_message(f"关闭高压失败: {e}")

        # UI归零
        try:
            self.keithley_voltage_label.setText("0.0 V")
        except Exception:
            pass
        try:
            # 确保稳流线程的电压显示也归零
            self.on_keithley_voltage_updated(0.0)
        except Exception:
            pass

        self.is_stabilizing = False
        self.start_stabilization_btn.setEnabled(True)
        self.stop_stabilization_btn.setEnabled(False)
        self.current_stabilization_btn.setEnabled(True)

    def on_keithley_voltage_updated(self, voltage):
        """Keithley电压更新回调"""
        # 更新电压显示
        self.keithley_voltage_label.setText(f"{voltage:.1f} V")

    def on_stabilization_complete(self):
        """进入稳定区间回调（不自动停止）"""
        self.log_message("电流已进入稳定区间（稳流继续运行，需手动停止）")

    def on_data_saved(self):
        """数据保存完成后的回调"""
        pass

    def on_data_converted(self):
        """数据转换完成后的回调"""
        try:
            ok = getattr(self.data_saver, 'last_convert_success', None)
            msg = getattr(self.data_saver, 'last_convert_message', '')
            if ok is True:
                self.log_message("CSV 统计/循环数据已生成")
            elif ok is False:
                self.log_message(f"生成 CSV 统计/循环数据失败: {msg}")
            else:
                self.log_message("临时数据转换完成")
        finally:
            self.is_converting = False
            try:
                self.record_btn.setEnabled(True)
            except Exception:
                pass

    def show_test_settings(self):
        """显示测试设置对话框"""
        dialog = TestSettingsDialog(self)

        # 设置当前值
        dialog.start_voltage_edit.setText(str(self.test_params['start_voltage']))
        dialog.target_voltage_edit.setText(str(self.test_params['target_voltage']))
        dialog.voltage_step_edit.setText(str(self.test_params['voltage_step']))
        dialog.step_delay_edit.setText(str(self.test_params['step_delay']))
        dialog.cycle_time_edit.setText(str(self.test_params['cycle_time']))

        if dialog.exec_() == QDialog.Accepted:
            # 保存设置
            try:
                self.test_params['start_voltage'] = float(dialog.start_voltage_edit.text())
                self.test_params['target_voltage'] = float(dialog.target_voltage_edit.text())
                self.test_params['voltage_step'] = float(dialog.voltage_step_edit.text())
                self.test_params['step_delay'] = float(dialog.step_delay_edit.text())
                self.test_params['cycle_time'] = float(dialog.cycle_time_edit.text())

                self.update_settings_display()
                self.log_message("测试参数已更新")

            except ValueError as e:
                self.log_message(f"参数设置错误: {str(e)}")

    def update_settings_display(self):
        """更新设置显示"""
        settings_text = f"起始: {self.test_params['start_voltage']}V, 目标: {self.test_params['target_voltage']}V, " \
                        f"步长: {self.test_params['voltage_step']}V, 延迟: {self.test_params['step_delay']}s"
        if self.test_params['cycle_time'] > 0:
            settings_text += f", 循环: {self.test_params['cycle_time']}s"

        self.current_settings_label.setText(f"当前设置: {settings_text}")

    def load_config_to_ui(self):
        """将配置加载到界面"""
        try:
            if self.config.has_option('HighVoltage', 'port'):
                port = self.config.get('HighVoltage', 'port')
                if port and port in [self.hv_port_combo.itemText(i) for i in range(self.hv_port_combo.count())]:
                    self.hv_port_combo.setCurrentText(port)

            if self.config.has_option('HighVoltage', 'baudrate'):
                baudrate = self.config.get('HighVoltage', 'baudrate')
                if baudrate and baudrate in [self.hv_baudrate_combo.itemText(i) for i in
                                             range(self.hv_baudrate_combo.count())]:
                    self.hv_baudrate_combo.setCurrentText(baudrate)

            # 修改：增加备用万用表的配置加载
            for meter_type in ['cathode', 'gate', 'anode', 'backup', 'vacuum']:
                port_key = f'{meter_type}_port'
                coeff_key = f'{meter_type}_coeff'

                if self.config.has_option('Multimeter', port_key):
                    port = self.config.get('Multimeter', port_key)
                    port_combo = getattr(self, f"{meter_type}_port_combo")
                    if port and port in [port_combo.itemText(i) for i in range(port_combo.count())]:
                        port_combo.setCurrentText(port)

                if self.config.has_option('Multimeter', coeff_key):
                    coeff = self.config.get('Multimeter', coeff_key)
                    coeff_edit = getattr(self, f"{meter_type}_coeff")
                    coeff_edit.setText(coeff)

            # 加载Keithley 248配置
            if self.config.has_option('Keithley248', 'gpib_address'):
                address = self.config.get('Keithley248', 'gpib_address')
                # 尝试设置下拉框
                found = False
                for i in range(self.keithley_addr_combo.count()):
                    if address in self.keithley_addr_combo.itemText(i):
                        self.keithley_addr_combo.setCurrentIndex(i)
                        found = True
                        break
                if not found:
                    self.keithley_addr_combo.setCurrentText(address)

            if self.config.has_option('Keithley248', 'current_source'):
                self.stabilization_params['current_source'] = self.config.get('Keithley248', 'current_source')

            if self.config.has_option('Keithley248', 'target_current'):
                self.stabilization_params['target_current'] = float(self.config.get('Keithley248', 'target_current'))

            if self.config.has_option('Keithley248', 'stability_range'):
                self.stabilization_params['stability_range'] = float(self.config.get('Keithley248', 'stability_range'))

            if self.config.has_option('Keithley248', 'start_voltage'):
                self.stabilization_params['start_voltage'] = float(self.config.get('Keithley248', 'start_voltage'))

            if self.config.has_option('Keithley248', 'adjust_frequency'):
                self.stabilization_params['adjust_frequency'] = float(
                    self.config.get('Keithley248', 'adjust_frequency'))

            if self.config.has_option('Keithley248', 'max_adjust_voltage'):
                self.stabilization_params['max_adjust_voltage'] = float(
                    self.config.get('Keithley248', 'max_adjust_voltage'))

            if self.config.has_option('Keithley248', 'algorithm'):
                algo = str(self.config.get('Keithley248', 'algorithm')).strip().lower()
                self.stabilization_params['algorithm'] = 'approach' if algo in ('approach','接近','接近算法') else 'pid'

            # 加载测试参数到对话框
            if self.config.has_option('TestParameters', 'start_voltage'):
                self.test_params['start_voltage'] = float(self.config.get('TestParameters', 'start_voltage'))
            if self.config.has_option('TestParameters', 'target_voltage'):
                self.test_params['target_voltage'] = float(self.config.get('TestParameters', 'target_voltage'))
            if self.config.has_option('TestParameters', 'voltage_step'):
                self.test_params['voltage_step'] = float(self.config.get('TestParameters', 'voltage_step'))
            if self.config.has_option('TestParameters', 'step_delay'):
                self.test_params['step_delay'] = float(self.config.get('TestParameters', 'step_delay'))
            if self.config.has_option('TestParameters', 'cycle_time'):
                self.test_params['cycle_time'] = float(self.config.get('TestParameters', 'cycle_time'))

            if self.config.has_option('TestParameters', 'save_interval'):
                interval = self.config.get('TestParameters', 'save_interval')
                self.interval_edit.setText(interval)

            if self.config.has_option('DataRecord', 'save_path'):
                path = self.config.get('DataRecord', 'save_path')
                if path and os.path.exists(os.path.dirname(path)):
                    self.path_label.setText(path)

            # Retention (SQLite maintenance) -> UI
            try:
                if hasattr(self, "db_keep_days_edit") and self.config.has_option("Retention", "keep_days"):
                    self.db_keep_days_edit.setText(self.config.get("Retention", "keep_days"))
                if hasattr(self, "db_keep_runs_edit") and self.config.has_option("Retention", "keep_runs"):
                    self.db_keep_runs_edit.setText(self.config.get("Retention", "keep_runs"))
                if hasattr(self, "db_archive_chk") and self.config.has_option("Retention", "archive_before_delete"):
                    v = str(self.config.get("Retention", "archive_before_delete")).strip().lower()
                    self.db_archive_chk.setChecked(v not in ("0", "false", "no"))
                if hasattr(self, "db_archive_dir_edit") and self.config.has_option("Retention", "archive_dir"):
                    self.db_archive_dir_edit.setText(self.config.get("Retention", "archive_dir"))
                if hasattr(self, "db_vacuum_mode_combo") and self.config.has_option("Retention", "vacuum_mode"):
                    vm = str(self.config.get("Retention", "vacuum_mode")).strip().lower()
                    idx = 0 if vm != "vacuum" else 1
                    self.db_vacuum_mode_combo.setCurrentIndex(idx)
                # refresh label
                try:
                    self.update_db_status_label()
                except Exception:
                    pass
            except Exception:
                pass

            self.log_message("配置已加载")

        except Exception as e:
            error_msg = f"加载配置错误: {str(e)}"
            self.log_message(error_msg)

    def save_config_from_ui(self):
        """从界面保存配置到文件"""
        try:
            config_data = {}

            config_data['HighVoltage'] = {
                'port': self.hv_port_combo.currentText(),
                'baudrate': self.hv_baudrate_combo.currentText()
            }

            config_data['Multimeter'] = {}
            # 修改：增加备用万用表的配置保存
            for meter_type in ['cathode', 'gate', 'anode', 'backup', 'vacuum']:
                port_combo = getattr(self, f"{meter_type}_port_combo")
                coeff_edit = getattr(self, f"{meter_type}_coeff")

                config_data['Multimeter'][f'{meter_type}_port'] = port_combo.currentText()
                config_data['Multimeter'][f'{meter_type}_coeff'] = coeff_edit.text()

            # 保存Keithley 248配置
            config_data['Keithley248'] = {
                'gpib_address': self.keithley_addr_combo.currentText(),
                'current_source': self.stabilization_params['current_source'],
                'target_current': str(self.stabilization_params['target_current']),
                'stability_range': str(self.stabilization_params['stability_range']),
                'start_voltage': str(self.stabilization_params['start_voltage']),
                'adjust_frequency': str(self.stabilization_params['adjust_frequency']),
                'max_adjust_voltage': str(self.stabilization_params['max_adjust_voltage']),
                'algorithm': str(self.stabilization_params.get('algorithm','pid'))
            }

            config_data['TestParameters'] = {
                'start_voltage': str(self.test_params['start_voltage']),
                'target_voltage': str(self.test_params['target_voltage']),
                'voltage_step': str(self.test_params['voltage_step']),
                'step_delay': str(self.test_params['step_delay']),
                'cycle_time': str(self.test_params['cycle_time']),
                'save_interval': self.interval_edit.text()
            }

            config_data['DataRecord'] = {
                'save_path': self.path_label.text()
            }

            # SQLite retention / maintenance settings (optional UI)
            try:
                if hasattr(self, "db_keep_days_edit") and hasattr(self, "db_keep_runs_edit"):
                    keep_days = int(float(self.db_keep_days_edit.text() or self.retention_policy.keep_days))
                    keep_runs = int(float(self.db_keep_runs_edit.text() or self.retention_policy.keep_runs))
                    archive_before_delete = bool(getattr(self, "db_archive_chk", None).isChecked()) if hasattr(self, "db_archive_chk") else bool(self.retention_policy.archive_before_delete)
                    vacuum_mode = str(getattr(self, "db_vacuum_mode_combo", None).currentData() or "incremental") if hasattr(self, "db_vacuum_mode_combo") else str(self.retention_policy.vacuum_mode)
                    config_data['Retention'] = {
                        'enabled': 'true',
                        'keep_days': str(keep_days),
                        'keep_runs': str(keep_runs),
                        'archive_before_delete': 'true' if archive_before_delete else 'false',
                        'archive_dir': str(getattr(self, "db_archive_dir_edit", None).text() if hasattr(self, "db_archive_dir_edit") else self.retention_policy.archive_dir),
                        'vacuum_mode': str(vacuum_mode)
                    }
            except Exception:
                pass

            self.config_manager.save_config(config_data)
            self.log_message("配置已保存")

        except Exception as e:
            error_msg = f"保存配置错误: {str(e)}"
            self.log_message(error_msg)

    def refresh_all_ports(self):
        """刷新串口与GPIB地址列表（整合刷新按钮）"""
        try:
            # 串口（高压源/万用表）
            self.refresh_ports()
            # GPIB（Keithley 248）
            self.refresh_gpib_ports()
            self.log_message("端口列表已刷新（串口 + GPIB）")
        except Exception as e:
            self.log_message(f"刷新端口列表失败: {str(e)}")

    def refresh_ports(self):
        """刷新所有串口列表"""
        try:
            ports = serial.tools.list_ports.comports()
            port_names = [port.device for port in ports]

            # 高压源：已连接时锁定下拉框，刷新时不改动其选项/当前值
            if self.hv_port_combo.isEnabled():
                current_hv_port = self.hv_port_combo.currentText()
                self.hv_port_combo.clear()
                self.hv_port_combo.addItems(port_names)
                if current_hv_port and current_hv_port in port_names:
                    self.hv_port_combo.setCurrentText(current_hv_port)

            # 万用表：已连接（下拉框禁用）时不刷新，避免程序性改动当前端口
            for meter_type in ['cathode', 'gate', 'anode', 'backup', 'vacuum']:
                combo = getattr(self, f"{meter_type}_port_combo")
                if not combo.isEnabled():
                    continue
                current_port = combo.currentText()
                combo.clear()
                combo.addItems(port_names)
                if current_port and current_port in port_names:
                    combo.setCurrentText(current_port)

            self.log_message(f"刷新串口列表，找到 {len(port_names)} 个可用串口")
        except Exception as e:
            self.log_message(f"刷新串口错误: {str(e)}")


    def toggle_hv_connection(self):
        """连接/断开高压源"""
        try:
            if self.hv_connect_btn.text() == "连接高压源":
                port = self.hv_port_combo.currentText()
                baudrate = int(self.hv_baudrate_combo.currentText())

                if not port:
                    self.log_message("错误: 请选择高压源串口")
                    return

                self.log_message(f"正在连接高压源: {port}, 波特率: {baudrate}...")
                self._start_hv_connection_async(port, baudrate)
            else:
                self.stop_test()
                self.countdown_manager.stop()
                self.countdown_label.setText("")

                self.stop_hv_voltage_poller()

                self._detach_hv_worker_signals()


                self.hv_controller.disconnect()
                # 断开后允许重新选择串口/波特率
                self.hv_port_combo.setEnabled(True)
                self.hv_baudrate_combo.setEnabled(True)
                try:
                    self.hv_refresh_btn.setEnabled(True)
                except Exception:
                    pass
                self.hv_connect_btn.setText("连接高压源")
                self.start_test_btn.setEnabled(False)
                self.cycle_test_btn.setEnabled(False)
                self.stop_test_btn.setEnabled(False)
                self.reset_btn.setEnabled(False)
                self.manual_set_btn.setEnabled(False)  # 禁用手动设置按钮
                self.hv_voltage_label.setText("未连接")
                self.hv_voltage_label.setStyleSheet("font-size: 11pt; font-weight: bold; color: #D32F2F; padding: 3px;")
                self.log_message("高压源已断开")
                self.status_bar.showMessage("高压源已断开")
        except Exception as e:
            error_msg = f"高压源连接/断开错误: {str(e)}"
            self.log_message(error_msg)

    def manual_set_voltage(self):
        """手动设置电压"""
        try:
            if not getattr(self.hv_controller, 'is_connected', False):
                self.log_message("错误: 高压源未连接")
                return

            voltage_str = self.manual_voltage_edit.text().strip()
            if not voltage_str:
                self.log_message("错误: 请输入电压值")
                return

            try:
                voltage = float(voltage_str)
                if voltage < 0 or voltage > 10000:
                    self.log_message("错误: 电压值应在0-10000V范围内")
                    return
            except ValueError:
                self.log_message("错误: 请输入有效的电压值")
                return

            # 如果正在测试，先停止测试
            if self.is_testing:
                self.log_message("警告: 正在测试中，将先停止测试")
                self.stop_test()

            self.log_message(f"正在手动设置电压: {voltage}V...")
            success, message = self.hv_controller.manual_set_voltage(voltage)
            if success:
                self.log_message(f"手动设置电压成功: {message}")
                self.manual_voltage_edit.clear()
                self.manual_voltage_edit.setFocus()
            else:
                self.log_message(f"手动设置电压失败: {message}")

        except Exception as e:
            error_msg = f"手动设置电压错误: {str(e)}"
            self.log_message(error_msg)

    def toggle_meter_connection(self, meter_type):
        """连接/断开万用表"""
        try:
            connect_btn = getattr(self, f"{meter_type}_connect_btn")
            value_label = getattr(self, f"{meter_type}_value_label")
            meter_names = {'cathode': '阴极', 'gate': '栅极', 'anode': '阳极', 'backup': '收集极', 'vacuum': '真空'}

            if connect_btn.text() == "连接":
                port_combo = getattr(self, f"{meter_type}_port_combo")
                port = port_combo.currentText()

                if not port:
                    self.log_message(f"错误: 请选择{meter_names[meter_type]}万用表串口")
                    return

                self.log_message(f"正在连接{meter_names[meter_type]}万用表: {port}...")
                if meter_type == 'vacuum':
                    # COMBIVAC CM52：主动查询线程
                    try:
                        channel = int(self.config.get('Multimeter', 'vacuum_channel', fallback='3'))
                    except Exception:
                        channel = 3
                    try:
                        baud = int(self.config.get('Multimeter', 'vacuum_baudrate', fallback='19200'))
                    except Exception:
                        baud = 19200
                    thread = CM52Thread(port=port, channel=channel, baudrate=baud, poll_ms=300)
                else:
                    thread = SerialThread(port, meter_type)

                thread.data_received.connect(self.handle_meter_data)
                thread.log_message_signal.connect(self.log_message)
                thread.start()

                self.meter_threads[meter_type] = thread
                connect_btn.setText("断开")
                # 连接后锁定串口下拉框，避免更改
                port_combo.setEnabled(False)
                value_label.setText("读取中...")
                self.log_message(f"{meter_names[meter_type]}万用表已连接到: {port}")
                self.status_bar.showMessage(f"{meter_names[meter_type]}万用表已连接")

            else:
                if meter_type in self.meter_threads:
                    thread = self.meter_threads[meter_type]
                    thread.data_received.disconnect()
                    thread.log_message_signal.disconnect()
                    thread.stop()
                    if thread.isRunning():
                        thread.wait(100)
                    del self.meter_threads[meter_type]

                port_combo = getattr(self, f"{meter_type}_port_combo")
                port_combo.setEnabled(True)

                connect_btn.setText("连接")
                value_label.setText("未连接")
                self.log_message(f"{meter_names[meter_type]}万用表已断开")
                self.status_bar.showMessage(f"{meter_names[meter_type]}万用表已断开")

        except Exception as e:
            error_msg = f"万用表连接/断开错误: {str(e)}"
            self.log_message(error_msg)

    def handle_meter_data(self, data):
        """处理万用表数据 - 优化性能"""
        try:
            meter_type = data['meter_name']
            coeff_edit = getattr(self, f"{meter_type}_coeff")

            try:
                coefficient = float(coeff_edit.text())
            except ValueError:
                coefficient = 1.0

            value = data['value'] * coefficient
            unit = data['unit']


            # 统一真空规单位：强制使用 Pa（避免出现 mbar / Torr / mPa 等显示）
            if meter_type == 'vacuum':
                try:
                    u = str(unit).strip().lower()
                except Exception:
                    u = 'pa'
                try:
                    v_raw = float(value)
                except Exception:
                    v_raw = value
                # 常见单位换算到 Pa
                if u in ('mbar', 'mb', 'millibar'):
                    value = float(v_raw) * 100.0
                    unit = 'Pa'
                elif u in ('bar',):
                    value = float(v_raw) * 1.0e5
                    unit = 'Pa'
                elif u in ('torr',):
                    value = float(v_raw) * 133.32236842105263
                    unit = 'Pa'
                elif u in ('mtorr',):
                    value = float(v_raw) * 0.13332236842105263
                    unit = 'Pa'
                elif u in ('pa',):
                    unit = 'Pa'
                else:
                    # 未知单位也按 Pa 显示（不改变数值），避免 UI 出现其他单位
                    unit = 'Pa'
            # 优化：减少锁的使用时间
            self.data_mutex.lock()
            self.meter_data[meter_type]['value'] = value
            self.meter_data[meter_type]['unit'] = unit
            self.meter_data[meter_type]['timestamp'] = time.time()
            self.meter_data[meter_type]['valid'] = True
            self.data_mutex.unlock()

            # 优化：使用队列更新显示，避免频繁的UI操作
            current_time = time.time()
            if current_time - self.last_meter_update_time > self.meter_update_interval:
                value_label = getattr(self, f"{meter_type}_value_label")
                value_label.setText(f"{value:.3e} {unit}" if meter_type=='vacuum' else f"{value:.4f} {unit}")
                self.last_meter_update_time = current_time

        except Exception as e:
            error_msg = f"处理万用表数据错误: {str(e)}"
            self.log_message(error_msg)

    def update_meter_displays(self):
        """定时更新万用表显示 - 优化性能"""
        try:
            for meter_type in ['cathode', 'gate', 'anode', 'backup', 'vacuum']:
                value_label = getattr(self, f"{meter_type}_value_label")
                # 直接从meter_data获取最新值，避免频繁的UI操作
                self.data_mutex.lock()
                value = self.meter_data[meter_type]['value']
                unit = self.meter_data[meter_type]['unit']
                self.data_mutex.unlock()

                # 只有在值变化时才更新显示
                current_text = value_label.text()
                new_text = (f"{value:.3e} {unit}" if meter_type=='vacuum' else f"{value:.4f} {unit}")
                if current_text != new_text:
                    value_label.setText(new_text)

        except Exception as e:
            # 避免频繁的错误日志
            pass



    def _attach_hv_worker_signals(self):
        """把高压源串口 worker 的信号接到日志，便于排障。"""
        try:
            w = getattr(self.hv_controller, "_worker", None)
            if not w:
                return
            # 先尝试断开，避免重复连接
            try:
                w.io_error.disconnect(self._on_hv_worker_error)
            except Exception:
                pass
            try:
                w.connected.disconnect(self._on_hv_worker_connected)
            except Exception:
                pass
            try:
                w.disconnected.disconnect(self._on_hv_worker_disconnected)
            except Exception:
                pass

            w.io_error.connect(self._on_hv_worker_error)
            w.connected.connect(self._on_hv_worker_connected)
            w.disconnected.connect(self._on_hv_worker_disconnected)
        except Exception as e:
            self.log_message(f"绑定高压源worker信号失败: {e}")

    def _detach_hv_worker_signals(self):
        try:
            w = getattr(self.hv_controller, "_worker", None)
            if not w:
                return
            try:
                w.io_error.disconnect(self._on_hv_worker_error)
            except Exception:
                pass
            try:
                w.connected.disconnect(self._on_hv_worker_connected)
            except Exception:
                pass
            try:
                w.disconnected.disconnect(self._on_hv_worker_disconnected)
            except Exception:
                pass
        except Exception:
            pass

    def _on_hv_worker_error(self, msg: str):
        self.log_message(f"[HAPS06] {msg}")

    def _on_hv_worker_connected(self, port: str):
        self.log_message(f"[HAPS06] 串口已连接: {port}")

    def _on_hv_worker_disconnected(self):
        self.log_message("[HAPS06] 串口已断开")


    def start_hv_voltage_poller(self, interval_ms: int = 500):
        """启动HAPS06后台轮询（实时刷新实际电压标签 & 数据）"""
        try:
            # 先停掉旧的
            self.stop_hv_voltage_poller()

            # 未连接则不启动
            if not (getattr(self.hv_controller, 'is_connected', False)):
                return

            self.hv_voltage_poller = HVVoltagePoller(self.hv_controller, interval_ms=interval_ms, parent=self)
            self.hv_voltage_poller.voltage_updated.connect(self.on_hv_voltage_polled)
            self.hv_voltage_poller.poll_error.connect(self._on_hv_poller_error)
            self.hv_voltage_poller.start()
        except Exception as e:
            self.log_message(f"启动高压源电压轮询失败: {e}")

    def stop_hv_voltage_poller(self):
        """停止HAPS06后台轮询"""
        try:
            if getattr(self, "hv_voltage_poller", None):
                try:
                    self.hv_voltage_poller.stop()
                except Exception:
                    pass
                self.hv_voltage_poller = None
        except Exception:
            pass

    def on_hv_voltage_polled(self, voltage: float):
        """后台轮询拿到的新电压（在主线程更新UI/缓存）"""
        try:
            v = float(voltage)
        except Exception:
            return
        self._hv_v_cache = v
        self._hv_v_ts = time.time()
        # controller 内部也会缓存 actual_voltage，这里再保险写一次
        try:
            self.hv_controller.actual_voltage = v
        except Exception:
            pass
        # UI实时更新
        self.update_hv_voltage_display(v)

    def _on_hv_poller_error(self, msg: str):
        # 错误节流已经在poller里做过，这里直接打日志即可
        try:
            self.log_message(str(msg))
        except Exception:
            pass

    def update_hv_voltage(self):
        """更新高压源电压（非阻塞）：只刷新显示，实际读取由后台线程完成"""
        try:
            if getattr(self.hv_controller, 'is_connected', False):
                # 优先用后台轮询缓存，避免主线程串口IO卡顿
                v = float(getattr(self, "_hv_v_cache", self.hv_controller.actual_voltage))
                self.update_hv_voltage_display(v)
            else:
                self.hv_voltage_label.setText("未连接")
                self.hv_voltage_label.setStyleSheet("font-size: 11pt; font-weight: bold; color: #D32F2F; padding: 3px;")
        except Exception:
            pass

    def update_hv_voltage_display(self, voltage):
        """更新高压源电压显示"""
        self.hv_voltage_label.setText(f"{voltage:.1f} V")
        self.hv_voltage_label.setStyleSheet("font-size: 11pt; font-weight: bold; color: #2E7D32; padding: 3px;")

    def update_status_display(self):
        """更新状态显示"""
        try:
            if self.countdown_manager.countdown == 0:
                self.countdown_label.setText("")
                if getattr(self.hv_controller, 'is_connected', False):
                    voltage = self.hv_controller.actual_voltage
                    self.status_bar.showMessage(f"高压源运行中 - 当前电压: {voltage:.1f} V")
                else:
                    self.status_bar.showMessage("系统运行中 - 未连接高压源")
        except Exception as e:
            error_msg = f"更新状态显示错误: {str(e)}"
            self.log_message(error_msg)

    def update_countdown_display(self, countdown):
        """更新倒计时显示"""
        try:
            if countdown > 0:
                self.countdown_label.setText(f"循环等待: {countdown}秒")
            else:
                self.countdown_label.setText("")
        except Exception as e:
            error_msg = f"更新倒计时显示错误: {str(e)}"
            self.log_message(error_msg)

    def update_plots(self):
        """更新图表 - 优化性能"""
        try:
            # 获取当前数据
            self.data_mutex.lock()
            cathode_val = self.meter_data['cathode']['value']
            gate_val = self.meter_data['gate']['value']
            anode_val = self.meter_data['anode']['value']
            backup_val = self.meter_data['backup']['value']
            vacuum_val = self.meter_data.get('vacuum', {}).get('value', 0.0)
            self.data_mutex.unlock()

            # 获取Keithley电压（缓存，避免每次都走GPIB导致卡顿）
            keithley_voltage = float(getattr(self, "_keithley_v_cache", 0.0))
            if self.keithley_controller.is_connected:
                now = time.time()
                ts = float(getattr(self, "_keithley_v_ts", 0.0))
                # 0.5s 内复用缓存；仍能保证实时性，但显著降低阻塞
                if now - ts >= 0.5:
                    v = self.keithley_controller.read_voltage()
                    if v is not None:
                        keithley_voltage = float(v)
                        self._keithley_v_cache = keithley_voltage
                        self._keithley_v_ts = now

            # 添加数据到缓冲区
            self.data_buffer.add_data(cathode_val, gate_val, anode_val, backup_val, keithley_voltage, vacuum_val)

            # Optional: write to InfluxDB for dashboards/diagnostics
            try:
                gate_plus_anode = float(gate_val) + float(anode_val) + float(backup_val)
            except Exception:
                gate_plus_anode = 0.0
            try:
                ratio = (float(anode_val) / float(cathode_val) * 100.0) if float(cathode_val) != 0.0 else 0.0
            except Exception:
                ratio = 0.0

            try:
                hv_port = self.hv_port_combo.currentText() if hasattr(self, "hv_port_combo") else ""
            except Exception:
                hv_port = ""
            try:
                keithley_addr = self.keithley_addr_combo.currentText() if hasattr(self, "keithley_addr_combo") else ""
            except Exception:
                keithley_addr = ""

            try:
                self.influx_writer.enqueue(
                    fields={
                        "cathode": float(cathode_val),
                        "gate": float(gate_val),
                        "anode": float(anode_val),
                        "backup": float(backup_val),
                        "vacuum": float(vacuum_val),
                        "keithley_voltage": float(keithley_voltage),
                        "hv_vout": float(hv_voltage),
                        "gate_plus_anode": float(gate_plus_anode),
                        "anode_cathode_ratio": float(ratio),
                        "is_testing": bool(self.is_testing),
                        "is_stabilizing": bool(self.is_stabilizing),
                        "is_recording": bool(self.is_recording),
                    },
                    tags={
                        "hv_port": hv_port,
                        "keithley": keithley_addr,
                        "session": str(self.session_id),
                        "run": str(self.current_run_id or ""),
                    },
                    timestamp_ns=time.time_ns(),
                )
            except Exception:
                pass

            # 获取绘图数据
            time_data, cathode_data, gate_data, anode_data, backup_data, keithley_voltage_data, vacuum_data, gate_plus_anode_data, anode_cathode_ratio_data = self.data_buffer.get_plot_data()

            # 更新图表
            self.plots['cathode'].setData(time_data, cathode_data)
            self.plots['gate'].setData(time_data, gate_data)
            self.plots['anode'].setData(time_data, anode_data)
            self.plots['backup'].setData(time_data, backup_data)
            self.plots['keithley_voltage'].setData(time_data, keithley_voltage_data)
            self.plots['gate_plus_anode'].setData(time_data, gate_plus_anode_data)
            self.plots['anode_cathode_ratio'].setData(time_data, anode_cathode_ratio_data)
            if 'vacuum' in self.plots:
                self.plots['vacuum'].setData(time_data, vacuum_data)

        except Exception as e:
            # 避免频繁的错误日志
            pass

    def start_test(self):
        """开始单次测试"""
        self.test_service.start(cycle=False)

    def start_cycle_test(self):
        """开始循环测试"""
        self.test_service.start(cycle=True)

    def _start_test(self, cycle=False):
        """开始测试的主逻辑"""
        try:
            start_v = self.test_params['start_voltage']
            target_v = self.test_params['target_voltage']
            step_v = self.test_params['voltage_step']
            delay = self.test_params['step_delay']
            cycle_time = self.test_params['cycle_time'] if cycle else 0

            if start_v == target_v:
                self.log_message("错误: 起始电压和目标电压不能相同")
                return

            if step_v <= 0:
                self.log_message("错误: 电压增幅必须大于0")
                return

            if start_v < target_v:
                self.test_mode = "升压"
                self.log_message(f"检测到升压测试模式: {start_v}V -> {target_v}V")
            else:
                self.test_mode = "降压"
                self.log_message(f"检测到降压测试模式: {start_v}V -> {target_v}V")

            self.is_testing = True
            self.is_cycle_testing = cycle
            self.start_test_btn.setEnabled(False)
            self.cycle_test_btn.setEnabled(False)
            self.stop_test_btn.setEnabled(True)
            self.manual_set_btn.setEnabled(False)  # 测试时禁用手动设置按钮

            # 单次测试时自动开始记录
            if not cycle and self.path_label.text() and self.path_label.text() != "未选择保存路径" and not self.is_recording:
                self.auto_recording = True
                self.toggle_record()
                self.log_message("单次测试已自动开始记录数据")

            # 循环测试时自动开始记录
            if cycle and self.path_label.text() and self.path_label.text() != "未选择保存路径" and not self.is_recording:
                self.auto_recording = True
                self.toggle_record()
                self.log_message("循环测试已自动开始记录数据")

            if cycle:
                self.current_cycle = 0
                self.cycle_data = []
                self.current_cycle_anode_data = []

            test_type = "循环测试" if cycle else "单次测试"
            self.log_message(f"开始{test_type}...")

            test_thread = threading.Thread(
                target=self.run_test,
                args=(start_v, target_v, step_v, delay, cycle_time, cycle),
            )
            test_thread.daemon = True
            test_thread.start()

        except ValueError as e:
            error_msg = f"测试参数错误: {str(e)}"
            self.log_message(error_msg)

    def run_test(self, start_voltage, target_voltage, voltage_step, step_delay, cycle_time, is_cycle):
        """运行测试（支持升压和降压模式）"""
        try:
            cycle_count = 0

            while self.is_testing and (is_cycle or cycle_count == 0):
                cycle_count += 1
                self.current_cycle = cycle_count
                self.log_message(f"开始第 {cycle_count} 轮测试")
                # 循环测试：写入CSV标记行用于分隔不同循环的数据
                if is_cycle and self.is_recording:
                    try:
                        self.data_saver.add_marker_row(f"第{cycle_count}次循环")
                        self.log_message(f"已写入第{cycle_count}次循环标记行")
                    except Exception as e:
                        self.log_message(f"写入循环标记行失败: {str(e)}")
                if is_cycle and self.is_recording:
                    self.current_cycle_anode_data = []

                # 设置起始电压
                success, message = self.hv_controller.set_voltage_only(start_voltage)
                if success:
                    self.log_message(f"设置起始电压: {start_voltage:.1f}V - {message}")
                    time.sleep(step_delay * 0.5)
                else:
                    self.log_message(f"设置起始电压失败: {message}")
                    break

                time.sleep(step_delay)

                # 循环测试：在测试期间激活记录
                if is_cycle and self.is_recording:
                    self.cycle_recording_active = True
                    self.log_message("测试期间数据记录已激活")

                if self.test_mode == "升压":
                    current_voltage = start_voltage
                    while current_voltage <= target_voltage and self.is_testing:
                        success, message = self.hv_controller.set_voltage_only(current_voltage)
                        if success:
                            self.log_message(f"设置电压: {current_voltage:.1f}V - {message}")
                        else:
                            self.log_message(f"设置电压失败: {message}")
                            break

                        current_voltage += voltage_step
                        time.sleep(step_delay)
                else:
                    current_voltage = start_voltage
                    while current_voltage >= target_voltage and self.is_testing:
                        success, message = self.hv_controller.set_voltage_only(current_voltage)
                        if success:
                            self.log_message(f"设置电压: {current_voltage:.1f}V - {message}")
                        else:
                            self.log_message(f"设置电压失败: {message}")
                            break

                        current_voltage -= voltage_step
                        time.sleep(step_delay)

                if not self.is_testing:
                    break

                # 循环测试：降到100V等待
                if is_cycle:
                    # 计算并保存当前循环的最小阳极值和对应电压
                    if self.is_recording and self.current_cycle_anode_data:
                        self.calculate_and_save_cycle_min()

                    # 循环测试：在等待期间暂停记录
                    if self.is_recording:
                        self.cycle_recording_active = False
                        self.log_message("等待期间数据记录已暂停")

                    self.log_message(f"到达目标电压，降至100V等待 {cycle_time} 秒")

                    drop_success, drop_message = self.hv_controller.set_voltage_only(100.0)
                    if drop_success:
                        self.log_message(f"电压已设置为100V - {drop_message}")

                        drop_wait_time = 0
                        max_drop_wait = 10
                        while drop_wait_time < max_drop_wait and self.is_testing:
                            set_voltage = self.hv_controller.read_set_voltage()
                            if set_voltage is not None and abs(set_voltage - 100.0) <= 1.0:
                                self.log_message(f"确认电压已设置为: {set_voltage:.1f}V (在容差范围内)")
                                break
                            time.sleep(1)
                            drop_wait_time += 1
                            self.log_message(f"等待电压设置... 当前设置电压: {set_voltage if set_voltage else '未知'}V")

                        if drop_wait_time >= max_drop_wait:
                            set_voltage = self.hv_controller.read_set_voltage()
                            self.log_message(
                                f"警告：电压设置等待超时，当前设置电压: {set_voltage if set_voltage else '未知'}V")
                    else:
                        self.log_message(f"电压下降失败: {drop_message}")

                    self.countdown_manager.start(int(cycle_time))

                    wait_start = time.time()
                    while time.time() - wait_start < cycle_time and self.is_testing:
                        time.sleep(0.5)
                        remaining = int(cycle_time - (time.time() - wait_start))
                        self.update_countdown_display(remaining)

                    self.countdown_manager.stop()
                else:
                    # 单次测试完成后降低到100V
                    self.log_message("单次测试完成，降低电压到100V")
                    drop_success, drop_message = self.hv_controller.set_voltage_only(100.0)
                    if drop_success:
                        self.log_message(f"电压已设置为100V - {drop_message}")
                    else:
                        self.log_message(f"电压下降失败: {drop_message}")

            self.is_testing = False
            self.is_cycle_testing = False

            # 循环测试完成后重置记录状态
            if is_cycle and self.is_recording:
                self.cycle_recording_active = False

            # 测试完成后自动停止记录
            if self.auto_recording and self.is_recording:
                self.auto_recording = False
                self.toggle_record()
                self.log_message("测试已完成，自动停止记录数据")

            QTimer.singleShot(0, self._update_ui_after_test)
        except Exception as e:
            error_msg = f"运行测试错误: {str(e)}"
            self.log_message(error_msg)

            # 异常情况下也停止自动记录
            if self.auto_recording and self.is_recording:
                self.auto_recording = False
                self.toggle_record()
                self.log_message("测试异常，自动停止记录数据")

            self.is_testing = False
            self.is_cycle_testing = False
            QTimer.singleShot(0, self._update_ui_after_test)

    def calculate_and_save_cycle_min(self):
        """计算并保存当前循环的最小阳极值和对应电压及时间"""
        try:
            if not self.current_cycle_anode_data:
                return

            min_anode = min(item[0] for item in self.current_cycle_anode_data)
            min_data = next(item for item in self.current_cycle_anode_data if item[0] == min_anode)
            min_voltage = min_data[1]
            min_time = min_data[2]

            self.cycle_data.append({
                'cycle': self.current_cycle,
                'min_anode': min_anode,
                'voltage': min_voltage,
                'time': min_time
            })
            # 将循环最小值写入 cycle.csv
            try:
                self.data_saver.append_cycle_row(self.current_cycle, min_anode, min_voltage, min_time)
                self.log_message(
                    f"第{self.current_cycle}次循环 - 最小阳极值: {min_anode:.4f}, 对应电压: {min_voltage}, 时间: {min_time}")
            except Exception as e:
                self.log_message(f"保存循环数据失败: {str(e)}")
        except Exception as e:
            error_msg = f"计算循环最小值失败: {str(e)}"
            self.log_message(error_msg)

    def _update_ui_after_test(self):
        """测试结束后更新UI"""
        self.stop_test_btn.setEnabled(False)
        self.start_test_btn.setEnabled(True)
        self.cycle_test_btn.setEnabled(True)
        self.manual_set_btn.setEnabled(True)  # 测试结束后重新启用手动设置按钮

        if not self.is_testing:
            self.log_message("测试完成")
        else:
            self.log_message("测试已停止")

    def stop_test(self):
        """停止测试"""
        try:
            self.test_service.stop()
        except Exception:
            self.is_testing = False
        self.is_cycle_testing = False
        self.countdown_manager.stop()
        self.countdown_label.setText("")

        # 停止测试时也停止自动记录
        if self.auto_recording and self.is_recording:
            self.auto_recording = False
            self.toggle_record()
            self.log_message("测试已停止，自动停止记录数据")

        self.hv_controller.stop_output()
        self._on_test_state_change({"testing": False})
        self.log_message("测试停止，输出电压已置零")

    def reset_voltage(self):
        """复位到100V"""
        success, message = self.hv_controller.reset_voltage()
        if success:
            self.log_message("已复位到100V")
        else:
            self.log_message(f"复位失败: {message}")

    def toggle_record(self):
        """开始/停止记录"""
        try:
            if self.record_btn.text() == "开始记录":
                if not self.path_label.text() or self.path_label.text() == "未选择保存路径":
                    self.log_message("错误: 请先选择保存路径")
                    return

                try:
                    # 创建新的CSV文件（写入表头）
                    csv_path = str(self.path_label.text())
                    os.makedirs(os.path.dirname(csv_path) or '.', exist_ok=True)
                    with open(csv_path, 'w', newline='', encoding='utf-8-sig') as _fh:
                        _w = csv.writer(_fh)
                        _w.writerow(list(DATA_HEADERS))

                    # DataSaver: use the selected CSV path for incremental writes
                    try:
                        self.data_saver.set_output_path(self.path_label.text())
                    except Exception:
                        pass

                    # InfluxDB: one bucket per run (named after the CSV filename stem)
                    # Helps manage and query each run independently.
                    try:
                        desired_bucket = self.influx_writer.set_bucket_for_csv(csv_path, create_if_missing=True)
                        if getattr(getattr(self, "influx_writer", None), "cfg", None) and bool(getattr(self.influx_writer.cfg, "enabled", False)):
                            actual_bucket = str(getattr(self.influx_writer.cfg, "bucket", "") or "")
                            err = str(getattr(self.influx_writer, "bucket_create_error", "") or "")
                            if actual_bucket == desired_bucket:
                                self.log_message(f"InfluxDB bucket已切换: {desired_bucket}")
                            else:
                                self.log_message(f"InfluxDB bucket创建失败，已回退到: {actual_bucket}（目标: {desired_bucket}）")
                                if err:
                                    self.log_message(f"原因: {err}")
                                self.log_message("提示：要自动创建 bucket，需要 InfluxDB Token 具备 buckets/orgs 的读写权限（或使用 All-Access Token）。")
                    except Exception as e:
                        if getattr(getattr(self, "influx_writer", None), "cfg", None) and bool(getattr(self.influx_writer.cfg, "enabled", False)):
                            self.log_message(f"InfluxDB bucket切换失败: {e}")

                    # 重置临时文件
                    # TEMP_DATA_FILE 已弃用：直接增量写入 Excel/SQLite
                    # self.data_saver._init_temp_file()

                    # --- SQLite: start a crash-safe run ---
                    try:
                        self.current_run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
                        self.sqlite_recorder.start_run(
                            self.current_run_id,
                            params={
                                "test_params": dict(self.test_params),
                                "stabilization_params": dict(self.stabilization_params),
                                "excel_path": str(self.path_label.text()),
                                "save_interval_s": float(self.interval_edit.text() or 1),
                            },
                        )
                    except Exception:
                        # Do not block acquisition if sqlite init fails
                        pass

                    # 启动保存定时器
                    interval = int(self.interval_edit.text()) * 1000
                    self.save_timer.start(interval)
                    self.is_recording = True
                    self.record_btn.setText("停止记录")

                    # 清空缓存数据
                    self.recorded_data.clear()
                    self.all_anode_data.clear()
                    self.anode_min_value = None
                    self.anode_min_voltage = None
                    self.anode_min_time = None
                    self.data_cache.clear()
                    try:
                        self._last_cache_send_ts = time.time()
                    except Exception:
                        pass

                    self.log_message("开始记录数据")
                    try:
                        self.log_message(f"SQLite 记录已开启: run={self.current_run_id}")
                    except Exception:
                        pass

                except Exception as e:
                    error_msg = f"开始记录失败: {str(e)}"
                    self.log_message(error_msg)
            else:
                # 先停定时器，避免停止过程中仍触发 save_data()
                self.save_timer.stop()

                # 关键：停止前先把缓存数据入队，否则后续 is_recording=False 会导致 flush 被跳过
                self.flush_data_cache(force=True)
                try:
                    self.data_saver.force_save()
                except Exception:
                    pass

                self.is_recording = False
                self.record_btn.setText("开始记录")

                # Stop SQLite run first (so last rows can still be committed in background)
                try:
                    self.sqlite_recorder.stop_run()
                except Exception:
                    pass
                self.current_run_id = ""

                # 强制保存剩余数据（防御性：此处再做一次，避免极端情况下仍有残留）
                self.flush_data_cache(force=True)
                try:
                    self.data_saver.force_save()
                except Exception:
                    pass

                # 生成 CSV 统计/循环数据（后台执行，避免数据量大时界面卡顿）
                csv_path = self.path_label.text()
                if os.path.exists(csv_path):
                    # 仅在主线程做“轻量计算”，写入交给后台线程处理
                    anode_min = None
                    if (not self.auto_recording) and self.all_anode_data:
                        try:
                            # 优先使用运行最小值（更快，且不受 deque 截断影响）
                            if self.anode_min_value is not None:
                                anode_min = {"min_anode": self.anode_min_value, "voltage": self.anode_min_voltage, "time": self.anode_min_time}
                            else:
                                min_anode = min(item[0] for item in self.all_anode_data)
                                min_data = next(item for item in self.all_anode_data if item[0] == min_anode)
                                anode_min = {"min_anode": min_anode, "voltage": min_data[1], "time": min_data[2]}
                        except Exception:
                            anode_min = None

                    cycle_data = list(self.cycle_data) if self.cycle_data else None

                    # 后台转换 + 写入统计 sheet
                    self.is_converting = True
                    try:
                        self.record_btn.setEnabled(False)
                    except Exception:
                        pass
                    self.log_message("正在后台生成 CSV 统计/循环数据（数据量大时需要一些时间）...")
                    self.data_saver.request_convert(csv_path, anode_min=anode_min, cycle_data=cycle_data)

                self.log_message("停止记录数据（转换在后台进行）")

                # Optional: retention auto-cleanup (safe trigger point)
                try:
                    self._maybe_auto_cleanup_sqlite()
                except Exception:
                    pass

        except Exception as e:
            error_msg = f"切换记录状态错误: {str(e)}"
            self.log_message(error_msg)

    def save_data(self):
        """保存数据 - 优化版本"""
        if not self.is_recording:
            return

        # 循环测试期间，只在测试阶段记录数据
        if self.is_cycle_testing and not self.cycle_recording_active:
            return

        try:
            current_time = datetime.now()
            current_time_str = current_time.strftime("%Y-%m-%d %H:%M:%S")
            hv_voltage = self.hv_controller.actual_voltage if (
                    getattr(self.hv_controller, 'is_connected', False)) else 0.0

            # 获取Keithley电压（缓存，避免每次都走GPIB导致卡顿）
            keithley_voltage = float(getattr(self, "_keithley_v_cache", 0.0))
            if self.keithley_controller.is_connected:
                now = time.time()
                ts = float(getattr(self, "_keithley_v_ts", 0.0))
                # 0.5s 内复用缓存；仍能保证实时性，但显著降低阻塞
                if now - ts >= 0.5:
                    v = self.keithley_controller.read_voltage()
                    if v is not None:
                        keithley_voltage = float(v)
                        self._keithley_v_cache = keithley_voltage
                        self._keithley_v_ts = now

            # 快速获取数据，减少锁时间
            self.data_mutex.lock()
            try:
                cathode_val = self.meter_data['cathode']['value']
                gate_val = self.meter_data['gate']['value']
                anode_val = self.meter_data['anode']['value']
                backup_val = self.meter_data['backup']['value']
                vacuum_val = self.meter_data.get('vacuum', {}).get('value', 0.0)
            finally:
                self.data_mutex.unlock()

            # 计算派生数据
            gate_plus_anode = gate_val + anode_val + backup_val
            anode_cathode_ratio = (anode_val / cathode_val * 100) if cathode_val != 0 else 0

            # 准备数据行（修改：增加栅极电压列）
            excel_row = [
                current_time_str,
                round(hv_voltage, 2) if hv_voltage is not None else "",
                round(cathode_val, 4),
                round(gate_val, 4),
                round(anode_val, 4),
                round(backup_val, 4),
                vacuum_val,
                round(keithley_voltage, 2),  # 新增：栅极电压
                round(gate_plus_anode, 4),
                round(anode_cathode_ratio, 2)
            ]


            # --- InfluxDB: write the *same* row that is being recorded to Excel (more reliable than plot timer) ---
            try:
                # tags
                try:
                    hv_port_tag = self.hv_port_combo.currentText() if hasattr(self, "hv_port_combo") else ""
                except Exception:
                    hv_port_tag = ""
                try:
                    keithley_tag = self.keithley_addr_combo.currentText() if hasattr(self, "keithley_addr_combo") else ""
                except Exception:
                    keithley_tag = ""
                self.influx_writer.enqueue(
                    fields={
                        "cathode": float(cathode_val),
                        "gate": float(gate_val),
                        "anode": float(anode_val),
                        "backup": float(backup_val),
                        "vacuum": float(vacuum_val),
                        "keithley_voltage": float(keithley_voltage),
                        "hv_vout": float(hv_voltage) if hv_voltage is not None else 0.0,
                        "gate_plus_anode": float(gate_plus_anode),
                        "anode_cathode_ratio": float(anode_cathode_ratio),
                        "is_testing": bool(self.is_testing),
                        "is_stabilizing": bool(self.is_stabilizing),
                        "is_recording": bool(self.is_recording),
                    },
                    tags={
                        "hv_port": str(hv_port_tag),
                        "keithley": str(keithley_tag),
                        "session": str(self.session_id),
                        "run": str(self.current_run_id or ""),
                    },
                    timestamp_ns=time.time_ns(),
                )
            except Exception:
                pass

            # --- SQLite: crash-safe local persistence (authoritative raw log) ---
            try:
                self.sqlite_recorder.enqueue_row(
                    ts_ms=int(time.time() * 1000),
                    row={
                        "time_text": current_time_str,
                        "hv_voltage": float(hv_voltage) if hv_voltage is not None else 0.0,
                        "cathode": float(cathode_val),
                        "gate": float(gate_val),
                        "anode": float(anode_val),
                        "backup": float(backup_val),
                        "vacuum": float(vacuum_val),
                        "keithley_voltage": float(keithley_voltage),
                        "gate_plus_anode": float(gate_plus_anode),
                        "anode_cathode_ratio": float(anode_cathode_ratio),
                    },
                )
            except Exception:
                pass

            # 添加到缓存
            self.data_cache.append(excel_row)

            # 保存到内存队列
            self.recorded_data.append(excel_row)
            # 运行最小值更新（优先用于最终统计，避免长测时 deque 截断导致不准）
            try:
                if self.anode_min_value is None or anode_val < self.anode_min_value:
                    self.anode_min_value = anode_val
                    self.anode_min_voltage = hv_voltage
                    self.anode_min_time = current_time_str
            except Exception:
                pass
            # 仍保留最近数据用于界面/调试（有上限）
            self.all_anode_data.append((anode_val, hv_voltage, current_time_str))

            if self.is_cycle_testing and self.is_recording:
                self.current_cycle_anode_data.append((anode_val, hv_voltage, current_time_str))

            # 批量发送策略：
            # 1) 达到 cache_size 立即入队
            # 2) 未达到 cache_size 也按时间间隔入队，避免长时间只写表头（尤其在停止前数据未触发阈值时）
            now_ts = time.time()
            if len(self.data_cache) >= self.cache_size:
                self.flush_data_cache()
            elif (now_ts - float(getattr(self, "_last_cache_send_ts", 0.0)) >= float(getattr(self, "cache_send_interval", 1.0))):
                self.flush_data_cache()

        except Exception as e:
            error_msg = f"准备保存数据失败: {str(e)}"
            self.log_message(error_msg)

    def flush_data_cache(self, force: bool = False):
        """将缓存的数据发送到保存线程（批量入队，减少 queue.put 次数，提升长测性能）。

        关键修复：
        - 停止记录时主流程会先把 is_recording 置 False（为了 UI 状态切换），
          这会导致最后一批 data_cache 无法入队，从而出现 CSV 只有表头。
        - 因此增加 force 参数：停止阶段可强制 flush，确保数据真正进入保存线程。
        """
        if not self.data_cache:
            return
        if (not self.is_recording) and (not force):
            return

        try:
            # 重要修复：必须发送 *拷贝*。
            # 之前直接把 self.data_cache 作为对象引用传给 DataSaver，然后立刻 clear()。
            # Queue 中保存的是同一个 list 对象引用，保存线程取出时该 list 已被清空，
            # 就会出现“CSV 只有表头、没有数据”的现象。
            rows_to_send = list(self.data_cache)
            self.data_cache.clear()

            # 批量发送到保存线程（传递拷贝，避免引用被清空）
            self.data_saver.add_batch(rows_to_send)

            # 更新节流时间戳（用于定时 flush，避免小批量长期不落盘）
            try:
                self._last_cache_send_ts = time.time()
            except Exception:
                pass

        except Exception as e:
            print(f"刷新数据缓存错误: {e}")

    def calculate_and_save_anode_min(self):
        """计算并保存阳极数据的最小值及对应时间（写入 summary.csv）"""
        try:
            if not self.all_anode_data:
                self.log_message("没有阳极数据可计算最小值")
                return

            min_anode = min(item[0] for item in self.all_anode_data)
            min_data = next(item for item in self.all_anode_data if item[0] == min_anode)
            min_voltage = min_data[1]
            min_time = min_data[2]

            if self.path_label.text() and self.path_label.text() != "未选择保存路径":
                anode_min = {"min_anode": min_anode, "voltage": min_voltage, "time": min_time}
                self.data_saver.request_convert(self.path_label.text(), anode_min=anode_min, cycle_data=list(self.cycle_data) if self.cycle_data else None)
                self.log_message(f"阳极最小值 - 值: {min_anode:.4f}, 电压: {min_voltage}, 时间: {min_time}")
        except Exception as e:
            self.log_message(f"计算阳极最小值失败: {str(e)}")

    def save_recorded_data(self):
        """保存记录的数据：生成 cycle.csv / summary.csv（如有）。"""
        if not self.recorded_data:
            return

        try:
            if self.path_label.text() and self.path_label.text() != "未选择保存路径":
                self.data_saver.request_convert(
                    self.path_label.text(),
                    anode_min=None,
                    cycle_data=list(self.cycle_data) if self.cycle_data else None,
                )
                if self.cycle_data:
                    self.log_message(f"已生成 {len(self.cycle_data)} 次循环的最小值数据（cycle.csv）")
            self.log_message("数据记录完成")
        except Exception as e:
            self.log_message(f"保存最终数据失败: {str(e)}")

    def select_path(self):
        """选择保存路径"""
        try:
            default_path = ""
            if self.path_label.text() and self.path_label.text() != "未选择保存路径":
                default_path = self.path_label.text()
            else:
                if self.config.has_option('DataRecord', 'save_path'):
                    default_path = self.config.get('DataRecord', 'save_path')

            if not default_path:
                default_path = f"测试数据_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

            path = QFileDialog.getSaveFileName(
                self,
                "保存文件",
                default_path,
                "CSV Files (*.csv)"
            )[0]

            if path:
                self.path_label.setText(path)
                self.log_message(f"数据保存路径: {path}")
                self.save_config_from_ui()

        except Exception as e:
            error_msg = f"选择保存路径错误: {str(e)}"
            self.log_message(error_msg)

    # ---------------- SQLite maintenance ----------------
    def get_sqlite_db_path(self) -> str:
        try:
            return str(self.sqlite_recorder.cfg.path)
        except Exception:
            return os.path.join("data", "session.sqlite")

    def get_db_stats(self) -> dict:
        """Return SQLite database stats for GUI/Web diagnostics."""
        try:
            return db_stats(self.get_sqlite_db_path())
        except Exception as e:
            return {"path": self.get_sqlite_db_path(), "error": str(e)}

    def cleanup_database(self, *, keep_days: int, keep_runs: int, archive_before_delete: bool, archive_dir: str, vacuum_mode: str):
        """Cleanup old SQLite runs.

        Safety: do not allow cleanup while recording.
        """
        if getattr(self, "is_recording", False):
            return {"ok": False, "message": "recording in progress; stop recording before cleanup", "data": None}

        # Best-effort: stop a possibly running sqlite writer thread and restart after cleanup
        try:
            self.sqlite_recorder.stop(timeout_s=2.0)
        except Exception:
            pass

        res = cleanup_db(
            self.get_sqlite_db_path(),
            keep_days=int(keep_days),
            keep_runs=int(keep_runs),
            archive_before_delete=bool(archive_before_delete),
            archive_dir=str(archive_dir),
            vacuum_mode=str(vacuum_mode or "incremental"),

        )
        try:
            self.sqlite_recorder.start()
        except Exception:
            pass

        return res

    def on_db_cleanup_clicked(self):
        """GUI handler: one-click cleanup."""
        try:
            keep_days = int(float(self.db_keep_days_edit.text() or self.retention_policy.keep_days))
            keep_runs = int(float(self.db_keep_runs_edit.text() or self.retention_policy.keep_runs))
            archive_before_delete = bool(self.db_archive_chk.isChecked())
            archive_dir = str(self.db_archive_dir_edit.text() or self.retention_policy.archive_dir)
            vacuum_mode = str(self.db_vacuum_mode_combo.currentData() or "incremental")
        except Exception:
            keep_days = int(self.retention_policy.keep_days)
            keep_runs = int(self.retention_policy.keep_runs)
            archive_before_delete = bool(self.retention_policy.archive_before_delete)
            archive_dir = str(self.retention_policy.archive_dir)
            vacuum_mode = str(self.retention_policy.vacuum_mode)

        res = self.cleanup_database(
            keep_days=keep_days,
            keep_runs=keep_runs,
            archive_before_delete=archive_before_delete,
            archive_dir=archive_dir,
            vacuum_mode=vacuum_mode,
        )
        try:
            if res.get("ok"):
                self.log_message(f"数据库清理完成: 删除 {res.get('data',{}).get('deleted_runs',0)} 个 run, {res.get('data',{}).get('deleted_rows',0)} 行")
            else:
                self.log_message(f"数据库清理失败: {res.get('message')}")
        except Exception:
            pass

        try:
            self.update_db_status_label()
        except Exception:
            pass

    def update_db_status_label(self):
        if not hasattr(self, "db_status_label"):
            return
        st = self.get_db_stats()
        size_mb = st.get("size_bytes", 0) / (1024 * 1024) if st.get("size_bytes") else 0.0
        runs = st.get("runs", "-")
        rows = st.get("rows", "-")
        msg = f"SQLite: {size_mb:.1f} MB, runs={runs}, rows={rows}"
        if st.get("error"):
            msg += f" (error: {st.get('error')})"
        self.db_status_label.setText(msg)

    def clear_plots(self):
        """清空图表"""
        try:
            self.data_buffer.clear()
            for plot in self.plots.values():
                plot.setData([], [])

            self.log_message("图表数据已清空")
        except Exception as e:
            error_msg = f"清空图表错误: {str(e)}"
            self.log_message(error_msg)

    def log_message(self, message):
        """记录消息"""
        try:
            timestamp = datetime.now().strftime("%H:%M:%S")
            formatted_message = f"[{timestamp}] {message}"
            self.log_text.append(formatted_message)
            if self.log_text.document().lineCount() > 1000:
                cursor = self.log_text.textCursor()
                cursor.movePosition(QtGui.QTextCursor.Start)
                cursor.movePosition(QtGui.QTextCursor.Down, QtGui.QTextCursor.KeepAnchor, 500)
                cursor.removeSelectedText()
            self.log_text.verticalScrollBar().setValue(self.log_text.verticalScrollBar().maximum())
        except Exception as e:
            print(f"记录消息错误: {str(e)}")

    def request_quit(self):
        """Request a full application shutdown (used by tray 'Quit')."""
        try:
            self._allow_quit = True
        except Exception:
            pass
        try:
            self.close()
        except Exception:
            try:
                QApplication.quit()
            except Exception:
                pass

    def closeEvent(self, event):
        """关闭程序时的清理工作"""
        # Tray mode: default behavior is to hide window instead of exiting.
        try:
            if not getattr(self, "_allow_quit", False):
                event.ignore()
                self.hide()
                try:
                    if self.tray_icon is not None:
                        self.tray_icon.showMessage(
                            "程序仍在运行",
                            "已最小化到系统托盘。可通过托盘菜单退出。",
                            getattr(self.tray_icon, "Information", 1),
                            3000,
                        )
                except Exception:
                    pass
                return
        except Exception:
            # If anything goes wrong, proceed with normal shutdown
            pass
        try:
            self.is_testing = False
            self.is_cycle_testing = False

            # 停止稳流控制
            if self.is_stabilizing:
                self.stop_current_stabilization()

            # 停止所有定时器
            self.hv_voltage_update_timer.stop()
            self.stop_hv_voltage_poller()
            self.keithley_voltage_update_timer.stop()
            self.data_update_timer.stop()
            self.status_update_timer.stop()
            self.meter_display_timer.stop()
            self.cache_flush_timer.stop()
            self.countdown_manager.stop()
            self.save_timer.stop()            # 保存最后的数据（尽量不阻塞：转换/写入交给后台线程）
            if self.is_recording:
                self.flush_data_cache()
                self.data_saver.force_save()

                csv_path = self.path_label.text()
                if os.path.exists(csv_path):
                    anode_min = None
                    if (not self.auto_recording) and self.all_anode_data:
                        try:
                            # 优先使用运行最小值（更快，且不受 deque 截断影响）
                            if self.anode_min_value is not None:
                                anode_min = {"min_anode": self.anode_min_value, "voltage": self.anode_min_voltage, "time": self.anode_min_time}
                            else:
                                min_anode = min(item[0] for item in self.all_anode_data)
                                min_data = next(item for item in self.all_anode_data if item[0] == min_anode)
                                anode_min = {"min_anode": min_anode, "voltage": min_data[1], "time": min_data[2]}
                        except Exception:
                            anode_min = None

                    cycle_data = list(self.cycle_data) if self.cycle_data else None

                    # 退出前同步等待转换完成，确保数据不丢（期间界面仍可响应）
                    self.is_converting = True
                    self.log_message("退出前：正在后台生成 CSV 统计/循环数据...")
                    self.data_saver.request_convert(csv_path, anode_min=anode_min, cycle_data=cycle_data)

                    try:
                        loop = QEventLoop()
                        timer = QTimer()
                        timer.setSingleShot(True)
                        timer.timeout.connect(loop.quit)
                        self.data_saver.convert_complete.connect(loop.quit)
                        timer.start(120000)  # 最多等 120 秒
                        loop.exec_()
                        timer.stop()
                    except Exception:
                        pass

            # 停止数据保存线程（放在转换之后）
            self.data_saver.stop()

            # Stop Influx writer (optional monitoring)
            try:
                self.influx_writer.stop()
            except Exception:
                pass
            # Stop SQLite recorder
            try:
                self.sqlite_recorder.stop_run()
            except Exception:
                pass
            try:
                self.sqlite_recorder.stop()
            except Exception:
                pass
            self.save_config_from_ui()

            # 断开设备连接
            self._detach_hv_worker_signals()

            self.hv_controller.disconnect()
            self.keithley_controller.disconnect()

            # 停止万用表线程
            for meter_type, thread in self.meter_threads.items():
                try:
                    thread.stop()
                except:
                    pass

            # 清理内存
            self.recorded_data.clear()
            self.all_anode_data.clear()
            self.data_cache.clear()
            gc.collect()

            self.log_message("系统已安全关闭")
            event.accept()
        except Exception as e:
            error_msg = f"关闭程序错误: {str(e)}"
            print(error_msg)
            event.accept()