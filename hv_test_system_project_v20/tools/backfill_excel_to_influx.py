from __future__ import annotations

import argparse
import configparser
import datetime as dt
from typing import Dict, Any, Optional

import pandas as pd
import requests



def parse_time_to_ns(v) -> Optional[int]:
    """Parse time value to epoch nanoseconds."""
    if v is None:
        return None
    if isinstance(v, dt.datetime):
        return int(v.timestamp() * 1_000_000_000)
    if isinstance(v, dt.date):
        # treat date-only as midnight
        return int(dt.datetime(v.year, v.month, v.day).timestamp() * 1_000_000_000)
    # string fallback
    try:
        s = str(v).strip()
        if not s:
            return None
        # accept 'YYYY-MM-DD HH:MM:SS'
        t = dt.datetime.fromisoformat(s)
        return int(t.timestamp() * 1_000_000_000)
    except Exception:
        return None


def lp_escape(s: str) -> str:
    return (s or "").replace("\\", "\\\\").replace(" ", "\\ ").replace(",", "\\,").replace("=", "\\=")


def field_fmt(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return f"{v}i"
    if isinstance(v, float):
        if v != v or v in (float('inf'), float('-inf')):
            return "0"
        return repr(float(v))
    # string field
    s = str(v).replace("\\", "\\\\").replace('"', '\\"')
    return f"\"{s}\""


def write_batch(url: str, org: str, bucket: str, token: str, lines: str) -> None:
    endpoint = url.rstrip("/") + "/api/v2/write"
    headers = {"Authorization": f"Token {token}"} if token else {}
    params = {"org": org, "bucket": bucket, "precision": "ns"}
    r = requests.post(endpoint, params=params, data=lines.encode("utf-8"), headers=headers, timeout=5)
    if r.status_code != 204:
        raise RuntimeError(f"Influx write failed: {r.status_code} {r.text}")


def main():
    ap = argparse.ArgumentParser(description="Backfill Excel records into InfluxDB v2")
    ap.add_argument("--excel", required=True, help="Path to exported Excel file")
    ap.add_argument("--config", default="config.ini", help="Path to config.ini")
    ap.add_argument("--url", default=None, help="Override InfluxDB URL")
    ap.add_argument("--org", default=None, help="Override InfluxDB org")
    ap.add_argument("--bucket", default=None, help="Override InfluxDB bucket")
    ap.add_argument("--token", default=None, help="Override InfluxDB token")
    ap.add_argument("--measurement", default=None, help="Override measurement (default hv_test)")
    ap.add_argument("--device", default=None, help="Override device tag")
    ap.add_argument("--session", default=None, help="Session tag")
    ap.add_argument("--run", default=None, help="Run tag")
    args = ap.parse_args()

    cfg = configparser.ConfigParser()
    cfg.read(args.config, encoding="utf-8")

    url = args.url or cfg.get("Monitoring", "influxdb_url", fallback="http://127.0.0.1:8086")
    org = args.org or cfg.get("Monitoring", "influxdb_org", fallback="")
    bucket = args.bucket or cfg.get("Monitoring", "influxdb_bucket", fallback="")
    token = args.token or cfg.get("Monitoring", "influxdb_token", fallback="")
    measurement = args.measurement or cfg.get("Monitoring", "influx_measurement", fallback="hv_test")
    device = args.device or cfg.get("Monitoring", "influx_device", fallback="")

    session = args.session or dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    run = args.run or "excel_import"

    if str(args.data).lower().endswith('.csv'):
        df = pd.read_csv(args.data)
    else:
        df = pd.read_csv(args.data)

    # column mapping (Excel -> influx fields)
    col = {
        "时间": "time",
        "高压源电压": "hv_vout",
        "阴极": "cathode",
        "栅极": "gate",
        "阳极": "anode",
        "收集极": "backup",
        "真空(Pa)": "vacuum",
        "栅极电压": "keithley_voltage",
        "栅极+阳极+收集极": "gate_plus_anode",
        "(阳极/阴极)×100": "anode_cathode_ratio",
    }

    # Build line protocol
    lines = []
    for _, row in df.iterrows():
        ts = parse_time_to_ns(row.get("时间"))
        if ts is None:
            continue

        tags = {
            "device": device,
            "session": session,
            "run": run,
            "source": "excel",
        }
        tag_str = ",".join([f"{lp_escape(k)}={lp_escape(str(v))}" for k, v in tags.items() if str(v)])
        tag_part = ("," + tag_str) if tag_str else ""

        fields: Dict[str, Any] = {}
        for excel_name, influx_name in col.items():
            if influx_name == "time":
                continue
            v = row.get(excel_name)
            if v is None:
                continue
            # ensure numeric where expected
            try:
                fields[influx_name] = float(v)
            except Exception:
                # keep as string field (rare)
                fields[influx_name] = str(v)

        # require at least one field
        if not fields:
            continue

        field_parts = []
        for k, v in fields.items():
            fv = field_fmt(v)
            if fv is not None:
                field_parts.append(f"{lp_escape(k)}={fv}")
        if not field_parts:
            continue

        line = f"{lp_escape(measurement)}{tag_part} " + ",".join(field_parts) + f" {ts}"
        lines.append(line)

        # flush in chunks to avoid huge payloads
        if len(lines) >= 5000:
            write_batch(url, org, bucket, token, "\n".join(lines))
            lines.clear()

    if lines:
        write_batch(url, org, bucket, token, "\n".join(lines))

    print(f"OK: imported rows into measurement={measurement}, bucket={bucket}, org={org}")


if __name__ == "__main__":
    main()
