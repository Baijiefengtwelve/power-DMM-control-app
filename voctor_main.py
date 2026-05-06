"""
VICTOR 86E 万用表 HID 采集程序（基于实测功能码）
实测功能码：电压0x3B，毫安0x3F，安培0x30
结束符：0x0D 0x0A
"""

import hid
import time
import sys

# 功能码（实测）
FUNC_VOLT = 0x3B
FUNC_MA   = 0x3F   # 原文档为0xBF，实测为0x3F
FUNC_A    = 0x30   # 原文档为0xB0，实测为0x30
# 微安档尚未实测，保留原值0x3D（如有需要可调整）
FUNC_UA   = 0x3D

# 量程映射（根据实测功能码调整）
RANGE_MAP = {
    FUNC_VOLT: {
        0x0: ("V", 10000, "2.2000V"),
        0x1: ("V", 1000,  "22.000V"),
        0x2: ("V", 100,   "220.00V"),
        0x3: ("V", 10,    "1000.0V"),
        0x4: ("mV", 100,  "220.00mV"),
    },
    FUNC_UA: {
        0x0: ("uA", 100, "220.00uA"),
        0x1: ("uA", 10,  "2200.0uA"),
    },
    FUNC_MA: {
        0x0: ("mA", 1000, "22.000mA"),
        0x1: ("mA", 100,  "220.00mA"),
    },
    FUNC_A: {
        0x0: ("A", 1000, "10.000A"),
    },
}

def parse_data(data: bytes):
    if len(data) < 14:
        return None, None, "数据长度不足"

    # 结束符：实测为 0x0D 0x0A
    if not (data[12] == 0x0D and data[13] == 0x0A):
        return None, None, f"无效结束符 {data[12]:02X} {data[13]:02X}"

    status = data[7]
    if status == 0x31:          # OL
        return None, None, "OL"
    sign = -1 if status == 0x34 else 1   # 0x34负，其余正

    func = data[6]
    if func not in RANGE_MAP:
        return None, None, f"非电压/电流档 (0x{func:02X})"

    range_idx = data[0] & 0x0F
    if range_idx not in RANGE_MAP[func]:
        return None, None, f"未知量程 {data[0]:02X}"

    unit, divisor, _ = RANGE_MAP[func][range_idx]

    raw = 0
    for i in range(1, 6):
        digit = data[i] & 0x0F
        if digit > 9:
            return None, None, f"无效数字位 {digit}"
        raw = raw * 10 + digit

    value = sign * raw / divisor
    return value, unit, "OK"

def find_device():
    for device in hid.enumerate():
        product = device.get('product_string', '')
        if '86E' in product or 'VICTOR' in product.upper():
            print(f"找到设备: {product}")
            return device['path']
    print("未找到VICTOR 86E")
    return None

def main():
    path = find_device()
    if not path:
        sys.exit(1)

    try:
        dev = hid.device()
        dev.open_path(path)
        print("设备已打开，开始采集电压/电流... (Ctrl+C退出)")
        dev.set_nonblocking(False)

        while True:
            report = dev.read(64, timeout_ms=1000)
            if report and len(report) >= 14:
                if report[0] == 0x00:
                    data = bytes(report[1:15])
                else:
                    data = bytes(report[:14])

                value, unit, msg = parse_data(data)
                if value is not None:
                    print(f"{value:.4f} {unit}")
                elif msg not in ("非电压/电流档", "OK"):
                    print(f"提示: {msg}")
            time.sleep(0.05)

    except KeyboardInterrupt:
        print("\n采集停止")
    except Exception as e:
        print(f"错误: {e}")
    finally:
        if 'dev' in locals():
            dev.close()

if __name__ == "__main__":
    main()