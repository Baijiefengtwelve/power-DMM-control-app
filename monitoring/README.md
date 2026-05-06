#  + InfluxDB（可选实时仪表盘）

本目录提供一个开箱即用的 Docker Compose：
- InfluxDB 2.x（用于存储时序数据）
- （用于可视化仪表盘）

> 该监控系统是可选功能。即使不使用该目录，桌面 GUI 与 Web UI 仍可正常运行。

## 1. 前置条件

- Windows 10 专业版
- 已安装 Docker Desktop（推荐）

## 2. 一键启动

1) 在本目录复制环境变量文件：

```
copy .env.example .env
```

2)（可选）修改 `.env` 中的账号密码、Token。

3) 启动：

```
docker compose up -d
```

启动后：
- InfluxDB: http://127.0.0.1:8086
- : http://127.0.0.1:3000

## 3. 将程序数据写入 InfluxDB

在项目根目录 `config.ini` 中启用：

```ini
[Monitoring]
enable_influxdb = true
influxdb_mode = v2
influxdb_url = http://127.0.0.1:8086
influxdb_org = hv_lab
influxdb_bucket = hv_test
influxdb_token = hv_test_token_change_me
influx_measurement = hv_test
influx_device = win10
```

注意：`influxdb_token` 必须与 `.env` 里的 `INFLUX_TOKEN` 保持一致。

## 4.  仪表盘

-  已通过 provisioning 自动配置数据源（InfluxDB_v2）
- 也会自动加载本目录的 dashboard：`grafana/dashboards/hv_test_dashboard.json`

如果你修改了 bucket/org/token：
- 同步修改 `grafana/provisioning/datasources/datasource.yaml`
- 或者在  UI 中手动编辑 Data Source

## 5. 停止与清理

停止：

```
docker compose down
```

停止并清理数据卷（会清空历史数据）：

```
docker compose down -v
```
