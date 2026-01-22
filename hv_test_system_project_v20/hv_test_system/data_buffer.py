from __future__ import annotations

from .common import *

class DataBuffer:
    """数据缓冲区，优化数据处理性能"""

    def __init__(self, max_points=3000):  # 减少最大点数，提高性能
        self.max_points = max_points
        self.time_data = np.zeros(max_points)
        self.cathode_data = np.zeros(max_points)
        self.gate_data = np.zeros(max_points)
        self.anode_data = np.zeros(max_points)
        self.backup_data = np.zeros(max_points)
        self.keithley_voltage_data = np.zeros(max_points)
        self.vacuum_data = np.zeros(max_points)  # 新增：真空  # 新增：Keithley电压数据
        self.gate_plus_anode_data = np.zeros(max_points)
        self.anode_cathode_ratio_data = np.zeros(max_points)
        self.index = 0
        self.is_full = False
        self.start_time = time.time()
        self.last_plot_update = 0
        self.plot_update_interval = 0.2  # 图表更新间隔0.2秒

    def add_data(self, cathode, gate, anode, backup, keithley_voltage, vacuum):
        """添加新数据点"""
        current_time = time.time() - self.start_time

        # 使用模运算实现循环缓冲区，避免数组拷贝
        idx = self.index % self.max_points

        self.time_data[idx] = current_time
        self.cathode_data[idx] = cathode
        self.gate_data[idx] = gate
        self.anode_data[idx] = anode
        self.backup_data[idx] = backup
        self.keithley_voltage_data[idx] = keithley_voltage  # 新增
        self.vacuum_data[idx] = vacuum  # 新增：真空
        self.gate_plus_anode_data[idx] = gate + anode + backup
        self.anode_cathode_ratio_data[idx] = (anode / cathode * 100) if cathode != 0 else 0

        self.index += 1
        if self.index >= self.max_points:
            self.is_full = True

    def get_plot_data(self):
        """获取绘图数据 - 优化版本"""
        if self.index == 0:
            return [np.array([])] * 9  # 修改：9个数组

        # 计算实际数据点数量
        actual_points = min(self.index, self.max_points)

        if self.is_full:
            # 数据已满，使用循环索引
            start_idx = self.index % self.max_points
            end_idx = (start_idx + self.max_points) % self.max_points

            # 获取时间数据并重新基准
            time_data = np.concatenate([
                self.time_data[start_idx:self.max_points],
                self.time_data[0:end_idx]
            ])

            # 重新基准时间
            time_data = time_data - time_data[0]

            # 获取其他数据
            cathode_data = np.concatenate([
                self.cathode_data[start_idx:self.max_points],
                self.cathode_data[0:end_idx]
            ])

            gate_data = np.concatenate([
                self.gate_data[start_idx:self.max_points],
                self.gate_data[0:end_idx]
            ])

            anode_data = np.concatenate([
                self.anode_data[start_idx:self.max_points],
                self.anode_data[0:end_idx]
            ])

            backup_data = np.concatenate([
                self.backup_data[start_idx:self.max_points],
                self.backup_data[0:end_idx]
            ])

            keithley_voltage_data = np.concatenate([
                self.keithley_voltage_data[start_idx:self.max_points],
                self.keithley_voltage_data[0:end_idx]
            ])

            vacuum_data = np.concatenate([
                self.vacuum_data[start_idx:self.max_points],
                self.vacuum_data[0:end_idx]
            ])

            gate_plus_anode_data = np.concatenate([
                self.gate_plus_anode_data[start_idx:self.max_points],
                self.gate_plus_anode_data[0:end_idx]
            ])

            anode_cathode_ratio_data = np.concatenate([
                self.anode_cathode_ratio_data[start_idx:self.max_points],
                self.anode_cathode_ratio_data[0:end_idx]
            ])

        else:
            # 数据未满
            time_data = self.time_data[:actual_points] - self.time_data[0]
            cathode_data = self.cathode_data[:actual_points]
            gate_data = self.gate_data[:actual_points]
            anode_data = self.anode_data[:actual_points]
            backup_data = self.backup_data[:actual_points]
            keithley_voltage_data = self.keithley_voltage_data[:actual_points]  # 新增
            vacuum_data = self.vacuum_data[:actual_points]
            gate_plus_anode_data = self.gate_plus_anode_data[:actual_points]
            anode_cathode_ratio_data = self.anode_cathode_ratio_data[:actual_points]

        return time_data, cathode_data, gate_data, anode_data, backup_data, keithley_voltage_data, vacuum_data, gate_plus_anode_data, anode_cathode_ratio_data

    def clear(self):
        """清空缓冲区"""
        self.time_data = np.zeros(self.max_points)
        self.cathode_data = np.zeros(self.max_points)
        self.gate_data = np.zeros(self.max_points)
        self.anode_data = np.zeros(self.max_points)
        self.backup_data = np.zeros(self.max_points)
        self.keithley_voltage_data = np.zeros(self.max_points)  # 新增
        self.gate_plus_anode_data = np.zeros(self.max_points)
        self.anode_cathode_ratio_data = np.zeros(self.max_points)
        self.index = 0
        self.is_full = False
        self.start_time = time.time()

