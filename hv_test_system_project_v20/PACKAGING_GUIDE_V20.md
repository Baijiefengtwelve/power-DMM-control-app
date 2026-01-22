# 打包指南（V20：GUI + Web + InfluxDB）

本项目推荐使用 **PyInstaller onedir** 方式打包，生成一个包含多个文件的文件夹（含可执行入口与支持文件）。
入口脚本：`run_gui_web_influx.py`

## 1. 环境准备

```bat
cd <项目根目录>
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
pip install pyinstaller
```

## 2. 推荐打包命令（Windows）

> 说明：`-w`（无控制台）模式下，Uvicorn 默认日志会尝试访问 `isatty()` 导致崩溃。V20 已在 `hv_test_system/service_manager.py` 中通过自定义 `log_config` 强制关闭颜色并落盘日志，确保无控制台环境可运行。

```bat
pyinstaller -D -w -n "采集控制程序V20" ^
  --clean ^
  --add-data "config.ini;." ^
  --add-data "hv_test_system\web\static;hv_test_system\web\static" ^
  --add-data "monitoring;monitoring" ^
  --add-data "tools;tools" ^
  --collect-submodules uvicorn ^
  --collect-submodules fastapi ^
  --collect-submodules starlette ^
  run_gui_web_influx.py
```

### 打包后目录结构自检

在 `dist\采集控制程序V20\` 下应至少包含：

- `采集控制程序V20.exe`
- `hv_test_system\web\static\index.html`
- `monitoring\docker-compose.yml`
- `launcher.log` / `web.log` / `influx.log`（运行后生成）

## 3. InfluxDB 启动策略与前置条件

`InfluxDBManager` 采用二选一启动：

1) **内置二进制（优先）**：若存在 `tools/influxdb/influxd.exe` 则直接启动（无需 Docker）
2) **Docker Compose（回退）**：若存在 `monitoring/docker-compose.yml` 则尝试 `docker compose up -d`

因此：
- 若目标机器没有 Docker Desktop，建议你在发行包中提供 `tools/influxdb/influxd.exe`
- Docker 模式启动/停止日志落在 `influx.log`

## 4. 常见问题排查

### Web UI not found
- 检查 `dist\...\hv_test_system\web\static\index.html` 是否存在
- 运行日志查看 `launcher.log` / `web.log`

### Influx 未启动
- 检查 `dist\...\monitoring\docker-compose.yml` 是否存在
- 查看 `influx.log` 是否有 docker 输出与返回码
- 确认目标机器 `docker compose version` 可用（Docker Desktop 已启动）
