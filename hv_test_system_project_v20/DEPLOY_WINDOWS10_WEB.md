# Windows 10 专业版部署指南（Web 局域网访问 + 控制）

本项目新增 `web_main.py`：在本机采集/控制串口设备，同时对局域网提供 Web 页面与 API。

---

## 1. 环境准备

### 1.1 安装 Python
建议 Python 3.10+（3.11/3.12 也可）。安装时勾选：
- Add Python to PATH

### 1.2 创建虚拟环境（推荐）
在项目根目录（含 `web_main.py`）打开 PowerShell：

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

---

## 2. 启动 Web 服务

### 2.1 仅 Web（后台）
```powershell
python web_main.py
```

### 2.2 Web + 桌面 GUI
```powershell
python web_main.py --gui
```

默认监听：`0.0.0.0:8000`

在本机浏览器打开：
- http://127.0.0.1:8000/

在同一局域网其它电脑打开：
- http://<本机IP>:8000/

> 本机 IP 可在 CMD 执行 `ipconfig` 查看（一般是 192.168.x.x 或 10.x.x.x）。

---

## 3. Windows 防火墙放行端口（必须）

### 3.1 图形界面方式
控制面板 → Windows Defender 防火墙 → 高级设置 → 入站规则 → 新建规则：
- 规则类型：端口
- TCP：8000（或你自定义端口）
- 允许连接
- 仅勾选：专用（Private）
- 名称：HV Test Web 8000

### 3.2 PowerShell 命令方式（可选）
管理员 PowerShell：
```powershell
New-NetFirewallRule -DisplayName "HV Test Web 8000" -Direction Inbound -Protocol TCP -LocalPort 8000 -Action Allow -Profile Private
```

---

## 4. 建议：固定 IP（可选但推荐）

为了让局域网访问地址稳定：
- 在路由器里给本机 MAC 地址做 DHCP 静态绑定
- 或在 Windows 网络适配器里手动设置固定 IPv4

---

## 5. 开机自启（两种方案）

### 5.1 任务计划程序（推荐，系统自带）
1. 打开「任务计划程序」
2. 创建任务（不是“基本任务”）
3. 触发器：登录时 或 开机时
4. 操作：启动程序
   - 程序/脚本：`<项目路径>\.venv\Scripts\python.exe`
   - 参数：`web_main.py`
   - 起始于：`<项目路径>`
5. 勾选：使用最高权限运行（如果串口权限/驱动需要）

### 5.2 NSSM 注册为 Windows 服务（更“服务化”）
1. 下载 NSSM（将 nssm.exe 放到某个目录）
2. 管理员命令行：
```cmd
nssm install HVTestWeb
```
3. 在弹窗里填写：
- Application：`<项目路径>\.venv\Scripts\python.exe`
- Arguments：`web_main.py`
- Startup directory：`<项目路径>`
4. 保存后启动服务：
```cmd
nssm start HVTestWeb
```

---

## 6. 使用说明（Web 端）

访问 `http://<本机IP>:8000/` 后可完成：
- 刷新端口 → 选择串口 → 连接/断开高压源、万用表
- 设置测试参数 → 开始测试/循环测试/停止
- Keithley 连接 → 开始/停止稳流
- 记录路径设置 → 开始/停止记录 → 下载 xlsx/csv 结果文件

---

## 7. 常见问题排查

1. **局域网打不开**
- 确认本机能访问 http://127.0.0.1:8000/
- 确认防火墙入站规则已放行
- 确认其它电脑与本机在同一网段（例如都在 192.168.1.x）

2. **InfluxDB/
通常需要额外完成以下步骤：

**(a) 启动 InfluxDB + 
在 `monitoring/` 目录执行：

```bat
copy .env.example .env
docker compose up -d
```

打开：
- InfluxDB: http://127.0.0.1:8086
- 
**(b) 在程序里启用写入并配置正确的 Token/Org/Bucket**

修改项目根目录 `config.ini`（`[Monitoring]`）：

```ini
enable_influxdb = true
influxdb_url = http://127.0.0.1:8086
influxdb_org = <与 .env 一致>
influxdb_bucket = <与 .env 一致>
influxdb_token = <与 .env 一致>
```

然后**重启** `python web_main.py`。

**(c) 用 Web 接口查看写入状态**

浏览器访问：
`http://127.0.0.1:8000/api/influx_status`

- `last_status=204` 表示写入成功
- `last_status=0/401/404` 等表示 URL/Token/Org/Bucket 配置错误

**(d) 若你只有 Excel 数据，需要回填到 InfluxDB**

使用工具：

```powershell
python tools\backfill_excel_to_influx.py --excel <你的文件.xlsx> --config config.ini
```

2. **端口占用**
- 改端口启动：`python web_main.py --port 8080`

3. **串口被占用**
- 确保没有其它软件占用串口
- 尽量只运行一个实例

4. **无外网环境**
- 本项目 Web UI 不依赖外部 CDN，静态资源已随项目提供。
