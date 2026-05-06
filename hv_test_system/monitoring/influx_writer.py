from __future__ import annotations

import os
import queue
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple
import re
from pathlib import Path

try:
    import requests  # type: ignore
except Exception:  # pragma: no cover
    requests = None  # type: ignore


def _escape_measurement(v: str) -> str:
    return (v or "").replace("\\", "\\\\").replace(" ", "\\ ").replace(",", "\\,")


def _escape_tag(v: str) -> str:
    return (v or "").replace("\\", "\\\\").replace(" ", "\\ ").replace(",", "\\,").replace("=", "\\=")


def _format_field_value(v: Any) -> str:
    # Influx line protocol field value rules
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return f"{v}i"
    if isinstance(v, float):
        if not (v == v) or v in (float("inf"), float("-inf")):
            return "0"
        return repr(float(v))
    # fallback string field (double-quoted)
    s = str(v).replace("\\", "\\\\").replace('"', '\\"')
    return f"\"{s}\""


@dataclass
class InfluxConfig:
    enabled: bool = False
    mode: str = "v2"
    url: str = "http://127.0.0.1:8086"
    org: str = ""
    bucket: str = ""
    token: str = ""
    token_source: str = "config"
    measurement: str = "hv_test"
    device: str = ""


class InfluxWriter:
    """
    Background InfluxDB v2 writer.

    - enqueue() is non-blocking (drops on full queue)
    - thread batches writes
    - transient network failures are retried briefly before data is dropped
    """

    MAX_WRITE_ATTEMPTS = 3
    RETRYABLE_STATUS_CODES = {0, 408, 409, 425, 429, 500, 502, 503, 504}
    TOKEN_ENV_VAR = "HV_INFLUX_TOKEN"
    TOKEN_ENV_PLACEHOLDER = "ENV:HV_INFLUX_TOKEN"

    def __init__(self, cfg: InfluxConfig):
        self.cfg = cfg
        # Bucket behavior
        self.default_bucket: str = (getattr(cfg, 'bucket', '') or 'hv_test').strip() or 'hv_test'
        self.desired_bucket: str = self.default_bucket
        self.bucket_create_error: str = ''
        self._q: "queue.Queue[Tuple[str, int, Dict[str, str], Dict[str, Any]]]" = queue.Queue(maxsize=20000)
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

        # Bucket management cache (InfluxDB v2)
        self._org_id: Optional[str] = None
        self._known_buckets: set[str] = set()
        self._bucket_lock = threading.RLock()

        # Diagnostics (useful when dashboards show "no data")
        self.last_write_ts: float = 0.0
        self.last_status: int = 0
        self.last_error: str = ""
        self.total_enqueued: int = 0
        self.total_dropped: int = 0
        self.total_retries: int = 0
        self.last_stop_ok: Optional[bool] = None
        self.last_stop_message: str = ""

        if self.cfg.enabled and requests is not None:
            self.start()

    # -------- bucket management (v2) --------

    @staticmethod
    def _sanitize_bucket_name(name: str) -> str:
        """Make a safe bucket name from an arbitrary filename stem.

        - Keep word characters (unicode letters/digits/_), dash, dot
        - Replace other chars with underscore
        - Collapse consecutive underscores
        """
        s = (name or "").strip()
        if not s:
            return "hv_test"
        s = re.sub(r"[^\w\-\.]", "_", s, flags=re.UNICODE)
        s = re.sub(r"_+", "_", s).strip("_")
        return s or "hv_test"

    def set_bucket_for_csv(self, csv_path: str, create_if_missing: bool = True) -> str:
        """Switch target bucket based on CSV filename (stem).

        Example: `D:\\data\\run_001.csv` -> bucket "run_001"
        """
        try:
            stem = Path(str(csv_path)).stem
        except Exception:
            stem = ""
        bucket = self._sanitize_bucket_name(stem)
        self.set_bucket(bucket, create_if_missing=create_if_missing)
        return bucket

    def set_bucket(self, bucket: str, create_if_missing: bool = True) -> bool:
        """Set active bucket (InfluxDB v2).

        Returns:
            True if the program will write into the requested bucket.
            False if bucket creation/check failed and the writer fell back to default_bucket.
        """
        b = self._sanitize_bucket_name(bucket)
        self.desired_bucket = b

        # If Influx is disabled, just update cfg and return.
        if not self.cfg.enabled or requests is None:
            self.cfg.bucket = b
            self.bucket_create_error = ""
            return True

        if self.cfg.mode.lower() != "v2":
            # v1 mode (or unknown): do not attempt bucket creation here.
            self.cfg.bucket = b
            self.bucket_create_error = ""
            return True

        if not create_if_missing:
            self.cfg.bucket = b
            self.bucket_create_error = ""
            return True

        # Best-effort: keep timeouts short and never block UI.
        ok, msg = self._ensure_bucket_exists_v2(b)
        if ok:
            self.cfg.bucket = b
            self.bucket_create_error = ""
            return True

        # Fallback: keep program usable even when token lacks bucket/org permissions.
        self.cfg.bucket = self.default_bucket
        self.bucket_create_error = msg or "bucket create/check failed"
        return False

    
    def _ensure_bucket_exists_v2(self, bucket: str) -> Tuple[bool, str]:
        """Create bucket if it does not exist (InfluxDB v2 HTTP API).

        Note: InfluxDB does NOT auto-create buckets on write. Bucket creation requires a token
        with buckets/orgs privileges. If privileges are insufficient, this returns (False, reason).
        """
        if requests is None:
            return False, "requests not available"

        # Fast path: cached
        with self._bucket_lock:
            if bucket in self._known_buckets:
                return True, ""

        # Resolve orgID
        org_name = (self.cfg.org or "").strip()
        org_id: Optional[str] = None

        if org_name:
            org_id = self._get_org_id_v2(org_name)
        else:
            org_id, org_name2 = self._get_first_org_v2()
            if org_id and org_name2:
                self.cfg.org = org_name2

        if not org_id:
            return False, "无法解析 InfluxDB 组织(org)：请在 config.ini 设置 influxdb_org，且 token 需具备 orgs 读取权限。"

        base = self.cfg.url.rstrip("/")
        headers = {"Authorization": f"Token {self.cfg.token}"} if self.cfg.token else {}

        # Check if bucket exists
        try:
            resp = requests.get(
                f"{base}/api/v2/buckets",
                params={"orgID": org_id, "name": bucket, "limit": 1},
                headers=headers,
                timeout=2.5,
            )
            status = int(getattr(resp, "status_code", 0) or 0)
            if status == 200:
                data = resp.json() if hasattr(resp, "json") else {}
                buckets = (data or {}).get("buckets") or []
                if buckets:
                    with self._bucket_lock:
                        self._known_buckets.add(bucket)
                    return True, ""
            elif status in (401, 403):
                try:
                    txt = (resp.text or "").strip()
                except Exception:
                    txt = ""
                return False, f"token 无权限查询 bucket（HTTP {status}）。请创建 All-Access Token 或授予 buckets/orgs 读写权限。{txt[:200]}"
        except Exception as e:
            # If list fails, we still try to create (idempotency depends on server)
            list_err = str(e)
        else:
            list_err = ""

        # Create bucket
        try:
            resp = requests.post(
                f"{base}/api/v2/buckets",
                json={"orgID": org_id, "name": bucket, "retentionRules": []},
                headers=headers,
                timeout=2.5,
            )
            status = int(getattr(resp, "status_code", 0) or 0)
            if status in (201, 200):
                with self._bucket_lock:
                    self._known_buckets.add(bucket)
                return True, ""
            # If bucket already exists, Influx may return 422; treat as ok.
            if status == 422:
                with self._bucket_lock:
                    self._known_buckets.add(bucket)
                return True, ""
            try:
                txt = (resp.text or "").strip()
            except Exception:
                txt = ""
            if status in (401, 403):
                return False, f"token 无权限创建 bucket（HTTP {status}）。请创建 All-Access Token 或授予 buckets 写权限。{txt[:200]}"
            return False, f"创建 bucket 失败（HTTP {status}）。{txt[:300]}" + (f"；list_err={list_err[:120]}" if list_err else "")
        except Exception as e:
            msg = str(e)
            return False, f"创建 bucket 异常：{msg[:300]}" + (f"；list_err={list_err[:120]}" if list_err else "")

    def _get_first_org_v2(self) -> Tuple[Optional[str], Optional[str]]:
        """Best-effort: return (org_id, org_name) for the first org visible to this token."""
        if requests is None:
            return None, None
        base = self.cfg.url.rstrip("/")
        headers = {"Authorization": f"Token {self.cfg.token}"} if self.cfg.token else {}
        try:
            resp = requests.get(
                f"{base}/api/v2/orgs",
                params={"limit": 1},
                headers=headers,
                timeout=2.5,
            )
            status = int(getattr(resp, "status_code", 0) or 0)
            if status != 200:
                return None, None
            data = resp.json() if hasattr(resp, "json") else {}
            orgs = (data or {}).get("orgs") or []
            if not orgs:
                return None, None
            org_id = orgs[0].get("id")
            org_name = orgs[0].get("name")
            if org_id and org_name:
                return str(org_id), str(org_name)
        except Exception:
            return None, None
        return None, None

    def _get_org_id_v2(self, org_name: str) -> Optional[str]:
        if requests is None:
            return None
        with self._bucket_lock:
            if self._org_id:
                return self._org_id

        base = self.cfg.url.rstrip("/")
        headers = {"Authorization": f"Token {self.cfg.token}"} if self.cfg.token else {}
        try:
            resp = requests.get(
                f"{base}/api/v2/orgs",
                params={"org": org_name, "limit": 1},
                headers=headers,
                timeout=2.5,
            )
            if int(getattr(resp, "status_code", 0) or 0) != 200:
                return None
            data = resp.json() if hasattr(resp, "json") else {}
            orgs = (data or {}).get("orgs") or []
            if not orgs:
                return None
            org_id = orgs[0].get("id")
            if org_id:
                with self._bucket_lock:
                    self._org_id = str(org_id)
                return str(org_id)
        except Exception:
            return None
        return None

    @classmethod
    def _resolve_token(cls, raw_token: str) -> tuple[str, str]:
        env_token = str(os.getenv(cls.TOKEN_ENV_VAR, "") or "").strip()
        if env_token:
            return env_token, "env"
        clean_token = str(raw_token or "").strip()
        if clean_token in {"", "CHANGE_ME", cls.TOKEN_ENV_PLACEHOLDER}:
            return "", "config"
        return clean_token, "config"

    @classmethod
    def from_config(cls, config) -> "InfluxWriter":
        cfg = InfluxConfig()
        try:
            cfg.enabled = bool(config.getboolean("Monitoring", "enable_influxdb", fallback=False))
            cfg.mode = str(config.get("Monitoring", "influxdb_mode", fallback="v2")).strip()
            cfg.url = str(config.get("Monitoring", "influxdb_url", fallback=cfg.url)).strip()
            cfg.org = str(config.get("Monitoring", "influxdb_org", fallback="")).strip()
            cfg.bucket = str(config.get("Monitoring", "influxdb_bucket", fallback="")).strip()
            cfg.token, cfg.token_source = cls._resolve_token(
                str(config.get("Monitoring", "influxdb_token", fallback="")).strip()
            )
            cfg.measurement = str(config.get("Monitoring", "influx_measurement", fallback=cfg.measurement)).strip()
            cfg.device = str(config.get("Monitoring", "influx_device", fallback="")).strip()
        except Exception:
            pass
        return cls(cfg)

    def start(self):
        if not self.cfg.enabled or requests is None:
            return False
        if self._thread and self._thread.is_alive():
            return True
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="InfluxWriter", daemon=True)
        self._thread.start()
        self.last_stop_ok = None
        self.last_stop_message = ""
        return True

    def stop(self, *, timeout_s: float = 5.0):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=timeout_s)
            if self._thread.is_alive():
                self.last_stop_ok = False
                self.last_stop_message = f"influx writer stop timed out after {float(timeout_s):.1f}s"
                self.last_error = self.last_stop_message
                return False
        self.last_stop_ok = True
        self.last_stop_message = ""
        return True

    def status(self) -> Dict[str, Any]:
        """Return last write status for debugging."""
        return {
            "enabled": bool(self.cfg.enabled and requests is not None),
            "url": self.cfg.url,
            "org": self.cfg.org,
            "bucket": self.cfg.bucket,
            "token_source": self.cfg.token_source,
            "measurement": self.cfg.measurement,
            "device": self.cfg.device,
            "queue_size": int(getattr(self._q, "qsize", lambda: 0)()),
            "last_write_ts": self.last_write_ts,
            "last_status": self.last_status,
            "last_error": self.last_error,
            "total_enqueued": self.total_enqueued,
            "total_dropped": self.total_dropped,
            "total_retries": self.total_retries,
            "last_stop_ok": self.last_stop_ok,
            "last_stop_message": self.last_stop_message,
        }

    def enqueue(
        self,
        fields: Dict[str, Any],
        tags: Optional[Dict[str, str]] = None,
        timestamp_ns: Optional[int] = None,
        measurement: Optional[str] = None,
    ) -> None:
        if not self.cfg.enabled or requests is None:
            return
        ts = int(timestamp_ns if timestamp_ns is not None else time.time_ns())
        m = measurement or self.cfg.measurement
        t = dict(tags or {})
        if self.cfg.device:
            t.setdefault("device", self.cfg.device)
        # Always tag run bucket name for easy filtering (even when fallback writes to default bucket)
        try:
            t.setdefault("run_bucket", str(getattr(self, 'desired_bucket', self.cfg.bucket) or self.cfg.bucket))
        except Exception:
            pass

        try:
            self._q.put_nowait((m, ts, t, dict(fields)))
            try:
                self.total_enqueued += 1
            except Exception:
                pass
        except queue.Full:
            self.total_dropped += 1
            self.last_error = "influx queue full, dropping"
        except Exception as e:
            self.last_error = str(e)[:500]

    def _build_line(self, measurement: str, ts: int, tags: Dict[str, str], fields: Dict[str, Any]) -> str:
        m = _escape_measurement(measurement)
        # tags
        tag_parts = []
        for k in sorted(tags.keys()):
            vv = tags.get(k)
            if vv is None or vv == "":
                continue
            tag_parts.append(f"{_escape_tag(str(k))}={_escape_tag(str(vv))}")
        tag_str = ("," + ",".join(tag_parts)) if tag_parts else ""

        # fields
        field_parts = []
        for k, v in fields.items():
            if v is None:
                continue
            field_parts.append(f"{_escape_tag(str(k))}={_format_field_value(v)}")
        if not field_parts:
            return ""
        field_str = ",".join(field_parts)
        return f"{m}{tag_str} {field_str} {ts}"

    def _note_drop(self, row_count: int, reason: str):
        self.total_dropped += max(0, int(row_count or 0))
        self.last_error = str(reason or "")[:500]

    def _should_retry_status(self, status_code: int) -> bool:
        return int(status_code or 0) in self.RETRYABLE_STATUS_CODES

    def _write_lines(self, lines: str, *, row_count: int = 0) -> bool:
        if not lines or requests is None:
            return False
        url = self.cfg.url.rstrip("/") + "/api/v2/write"
        headers = {"Authorization": f"Token {self.cfg.token}"} if self.cfg.token else {}
        params = {"org": self.cfg.org, "bucket": self.cfg.bucket, "precision": "ns"}
        for attempt in range(1, self.MAX_WRITE_ATTEMPTS + 1):
            try:
                resp = requests.post(url, params=params, data=lines.encode("utf-8"), headers=headers, timeout=2.5)
                self.last_write_ts = time.time()
                self.last_status = int(getattr(resp, "status_code", 0) or 0)
                if self.last_status == 204:
                    self.last_error = ""
                    return True

                try:
                    txt = (resp.text or "").strip()
                except Exception:
                    txt = ""
                self.last_error = txt[:500]
                if attempt < self.MAX_WRITE_ATTEMPTS and self._should_retry_status(self.last_status):
                    self.total_retries += 1
                    time.sleep(0.2 * attempt)
                    continue
                self._note_drop(row_count, self.last_error or f"HTTP {self.last_status}")
                return False
            except Exception as e:
                self.last_write_ts = time.time()
                self.last_status = 0
                self.last_error = str(e)[:500]
                if attempt < self.MAX_WRITE_ATTEMPTS:
                    self.total_retries += 1
                    time.sleep(0.2 * attempt)
                    continue
                self._note_drop(row_count, self.last_error)
                return False
        return False

    def _run(self):
        batch: list[Tuple[str, int, Dict[str, str], Dict[str, Any]]] = []
        last_flush = time.time()
        while not self._stop.is_set():
            try:
                item = self._q.get(timeout=0.25)
                batch.append(item)
            except Exception:
                pass

            now = time.time()
            if len(batch) >= 250 or (batch and (now - last_flush) >= 1.0):
                try:
                    lines = []
                    for m, ts, tags, fields in batch:
                        line = self._build_line(m, ts, tags, fields)
                        if line:
                            lines.append(line)
                    if lines:
                        self._write_lines("\n".join(lines), row_count=len(lines))
                finally:
                    batch.clear()
                    last_flush = now

        # flush remaining
        if batch:
            try:
                lines = []
                for m, ts, tags, fields in batch:
                    line = self._build_line(m, ts, tags, fields)
                    if line:
                        lines.append(line)
                if lines:
                    self._write_lines("\n".join(lines), row_count=len(lines))
            except Exception:
                pass
