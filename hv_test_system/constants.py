from __future__ import annotations


CONFIG_FILE = "config.ini"
TEMP_DATA_FILE = "临时数据.txt"

DATA_HEADERS = [
    "时间",
    "升压测试电源名称",
    "升压测试电压(V)",
    "阴极值",
    "阴极单位",
    "栅极值",
    "栅极单位",
    "阳极值",
    "阳极单位",
    "收集极值",
    "收集极单位",
    "真空(Pa)",
    "稳流测试电源名称",
    "稳流测试电压(V)",
    "栅极+阳极+收集极",
    "合成单位",
    "(阳极/阴极)×100(%)",
]

DATA_HEADER_LINE = ",".join(DATA_HEADERS) + "\n"
