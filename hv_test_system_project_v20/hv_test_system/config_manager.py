from __future__ import annotations

from .common import *
from .constants import CONFIG_FILE

class ConfigManager:
    """配置文件管理器"""

    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config_file = CONFIG_FILE

    def load_config(self):
        """加载配置文件"""
        if not os.path.exists(self.config_file):
            self.create_default_config()

        self.config.read(self.config_file, encoding='utf-8')
        return self.config

    def save_config(self, config_data):
        """保存配置到文件"""
        for section, options in config_data.items():
            if not self.config.has_section(section):
                self.config.add_section(section)
            for key, value in options.items():
                self.config.set(section, key, str(value))

        with open(self.config_file, 'w', encoding='utf-8') as f:
            self.config.write(f)

    def create_default_config(self):
        """创建默认配置文件"""
        # 高压电源设置
        self.config['HighVoltage'] = {
            'port': '',
            'baudrate': '9600'
        }

        # 万用表设置
        self.config['Multimeter'] = {
            'cathode_port': '',
            'cathode_coeff': '1.0',
            'gate_port': '',
            'gate_coeff': '1.0',
            'anode_port': '',
            'anode_coeff': '1.0',
            'backup_port': '',
            'backup_coeff': '1.0'
        ,
            'vacuum_port': '',
            'vacuum_coeff': '1.0',
            'vacuum_channel': '3',
            'vacuum_baudrate': '19200'
        }
# Keithley 248高压源设置
        self.config['Keithley248'] = {
            'gpib_address': '14',
            'current_source': 'cathode',
            'target_current': '1000',
            'stability_range': '5',
            'start_voltage': '100',
            'adjust_frequency': '1',
            'max_adjust_voltage': '50'
        }

        # 测试参数
        self.config['TestParameters'] = {
            'start_voltage': '0',
            'target_voltage': '1000',
            'voltage_step': '10',
            'step_delay': '1',
            'cycle_time': '10',
            'save_interval': '1'
        }

        # 数据记录
        self.config['DataRecord'] = {
            'save_path': ''
        }

        # 图表曲线颜色（可在UI中配置）
        self.config['PlotColors'] = {
            'cathode': '#E74C3C',
            'gate': '#2ECC71',
            'anode': '#3498DB',
            'backup': '#F39C12',
            'keithley_voltage': '#9B59B6',
            'gate_plus_anode': '#E67E22',
            'anode_cathode_ratio': '#1ABC9C'
        ,
            'vacuum': '#7F8C8D'
        }

        # Optional monitoring (InfluxDB)
        # - Disabled by default
        # - If you use monitoring/docker-compose.yml, copy the InfluxDB token into influxdb_token, and set enable_influxdb=true
        self.config['Monitoring'] = {
            'enable_influxdb': 'false',
            'influxdb_mode': 'v2',
            'influxdb_url': 'http://127.0.0.1:8086',
            'influxdb_org': 'hv_lab',
            'influxdb_bucket': 'hv_test',
            'influxdb_token': 'CHANGE_ME',
            'influxdb_database': 'hv_test',
            'influx_measurement': 'hv_test',
            'influx_device': 'win10',
            'influx_batch_size': '100',
            'influx_flush_interval_s': '1.0',
            'influx_timeout_s': '3.0'
        }

        # SQLite local persistence (recommended for crash-safe acquisition)
        self.config['SQLite'] = {
            'path': os.path.join('data', 'session.sqlite'),
            'journal_mode': 'WAL',
            'synchronous': 'NORMAL',
            'auto_vacuum': 'INCREMENTAL',
            'commit_every_rows': '200',
            'commit_every_ms': '500'
        }

        # Retention / maintenance policy for SQLite
        self.config['Retention'] = {
            'enabled': 'true',
            'keep_days': '30',
            'keep_runs': '200',
            'archive_before_delete': 'true',
            'archive_dir': os.path.join('data', 'archive'),
            'vacuum_mode': 'incremental'  # incremental | vacuum
        }
        with open(self.config_file, 'w', encoding='utf-8') as f:
            self.config.write(f)

