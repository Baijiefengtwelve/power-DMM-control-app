from __future__ import annotations

CONFIG_FILE = "config.ini"
TEMP_DATA_FILE = "临时数据.txt"
DATA_HEADERS = ['时间', '高压源电压', '阴极', '栅极', '阳极', '收集极', '真空(Pa)', '栅极电压', '栅极+阳极+收集极', '(阳极/阴极)×100']
DATA_HEADER_LINE = ",".join(DATA_HEADERS) + "\n"
