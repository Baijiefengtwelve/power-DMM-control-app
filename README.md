# 高压电源与万用表测试系统（模块化版）

## 运行
```bash
python main.py
```

## 依赖（与你原脚本一致）
- pyserial
- pyqt5
- pyqtgraph
- numpy
- openpyxl
- pyvisa（若使用 Keithley 248 的 GPIB 功能）



## Web 服务（局域网访问与控制）

### 1. 启动方式

- **仅 Web（后台运行，不显示桌面窗口）**
```bash
python web_main.py
```

- **Web + 桌面 GUI 同时运行**
```bash
python web_main.py --gui
```

默认监听 `0.0.0.0:8000`，同一局域网内的其它电脑可通过：
`http://<本机IP>:8000/` 访问控制页面。

可自定义端口：
```bash
python web_main.py --host 0.0.0.0 --port 8000
```

### 2. Web 功能覆盖

- 刷新/选择串口并连接/断开 HAPS06 高压源
- 连接/断开 Keithley 248（GPIB）
- 万用表（阴极/栅极/阳极/收集极/真空）连接/断开
- 测试参数配置、开始单次/循环测试、停止测试、复位（100V）
- 稳流（开始/停止）
- 记录路径设置、开始/停止记录、导出文件下载
- 读取实时数据与简易曲线显示（WebSocket 推送）

> 说明：Web 端通过线程安全桥接把操作指令排队到 Qt 主线程执行，保证与原桌面程序一致的设备控制逻辑。

## 可选：
若你希望在浏览器端使用更强的可视化（多图联动、历史回放、告警等），可启用 InfluxDB 写入，并用 
1) 启动监控栈：请查看 `monitoring/README.md`（Docker Compose 一键启动）
2) 在 `config.ini` 的 `[Monitoring]` 中设置 `enable_influxdb = true` 并填入 token/org/bucket

