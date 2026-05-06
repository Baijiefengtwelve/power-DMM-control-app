from __future__ import annotations

import math
import time
from datetime import datetime


class TestRuntimeService:
    """Handle lightweight UI/runtime reactions to test-service signals."""

    CHANNEL_SUMMARY_FIELDS = (
        ("阴极", 3, 4),
        ("栅极", 5, 6),
        ("阳极", 7, 8),
        ("收集极", 9, 10),
        ("真空", 11, None),
    )

    def __init__(self, mw):
        self.mw = mw

    def handle_state_change(self, state: dict):
        mw = self.mw
        try:
            if "cycle" in state:
                mw._last_test_was_cycle = bool(state.get("cycle"))
        except Exception:
            pass

        try:
            if state.get("countdown_stop"):
                mw.countdown_manager.stop()
                mw.update_countdown_display(0)
            if "countdown_start" in state:
                seconds = int(state.get("countdown_start") or 0)
                mw.countdown_manager.stop()
                mw.countdown_manager.countdown = max(0, seconds)
                mw.update_countdown_display(max(0, seconds))
            if "countdown_tick" in state:
                seconds = int(state.get("countdown_tick") or 0)
                mw.countdown_manager.countdown = max(0, seconds)
                mw.update_countdown_display(max(0, seconds))
            elif "countdown_start" in state:
                seconds = int(state.get("countdown_start") or 0)
                if seconds > 0:
                    mw.update_countdown_display(seconds)
        except Exception:
            pass

        testing = bool(state.get("testing", False))
        try:
            if testing != getattr(mw, "_prev_testing", False):
                mw._prev_testing = testing
        except Exception:
            pass
        try:
            stabilizing = bool(getattr(mw, "is_stabilizing", False))
            if stabilizing != getattr(mw, "_prev_stabilizing", False):
                mw._prev_stabilizing = stabilizing
        except Exception:
            pass
        try:
            recording = bool(getattr(mw, "is_recording", False))
            if recording != getattr(mw, "_prev_recording", False):
                mw._prev_recording = recording
        except Exception:
            pass

        try:
            mw.update_power_action_buttons()
        except Exception:
            pass

    def handle_finished(self):
        mw = self.mw
        try:
            was_cycle = bool(getattr(mw, "_last_test_was_cycle", getattr(mw, "is_cycle_testing", False)))

            try:
                mw.countdown_manager.stop()
                mw.update_countdown_display(0)
            except Exception:
                pass

            if getattr(mw, "auto_recording", False) and mw.is_recording:
                mw.auto_recording = False
                mw.toggle_record()
                mw.log_message("测试结束：已自动停止记录")

            summary_text = self._build_run_summary(was_cycle)
            if summary_text:
                mw.last_test_run_summary = summary_text
                for line in summary_text.splitlines():
                    mw.log_message(line)
        except Exception:
            pass

    def _build_run_summary(self, was_cycle: bool) -> str:
        mw = self.mw
        start_ts = float(getattr(mw, "test_run_started_at", 0.0) or 0.0)
        end_ts = time.time()
        duration_s = max(0.0, end_ts - start_ts) if start_ts > 0 else 0.0
        started_text = (
            datetime.fromtimestamp(start_ts).strftime("%Y-%m-%d %H:%M:%S")
            if start_ts > 0
            else "未知"
        )
        ended_text = datetime.fromtimestamp(end_ts).strftime("%Y-%m-%d %H:%M:%S")
        source_name = str(getattr(mw, "test_run_source_name", "") or "未知电源")
        cycle_count = self._completed_cycle_count(was_cycle)
        stats_lines = self._build_channel_summary_lines()
        stab_line = self._build_stabilization_summary_line()
        data_path = self._resolve_data_path()
        min_line = self._build_anode_minimum_line()

        lines = [
            "测试运行摘要:",
            f"开始={started_text}, 结束={ended_text}, 时长={duration_s:.1f}s",
            f"模式={'循环测试' if was_cycle else '单次测试'}, 电源={source_name}, 完成循环数={cycle_count}",
            stab_line,
        ]
        if min_line:
            lines.append(min_line)
        if stats_lines:
            lines.extend(stats_lines)
        else:
            lines.append("通道统计: 当前没有已记录样本，未生成统计值")
        lines.append(f"数据文件={data_path}")
        return "\n".join(lines)

    def _completed_cycle_count(self, was_cycle: bool) -> int:
        if not was_cycle:
            return 1
        try:
            return int(len(getattr(self.mw, "cycle_data", []) or []))
        except Exception:
            return 0

    def _build_stabilization_summary_line(self) -> str:
        mw = self.mw
        baseline = dict(getattr(mw, "_test_run_stabilization_baseline", {}) or {})
        completion_now = int(getattr(mw, "stabilization_completion_count", 0) or 0)
        failure_now = int(getattr(mw, "stabilization_failure_count", 0) or 0)
        completion_delta = completion_now - int(baseline.get("completion", 0) or 0)
        failure_delta = failure_now - int(baseline.get("failure", 0) or 0)
        return f"稳流收敛/失败={max(0, completion_delta)}/{max(0, failure_delta)}"

    def _build_anode_minimum_line(self) -> str:
        mw = self.mw
        try:
            min_anode = getattr(mw, "anode_min_value", None)
            if min_anode is None:
                return ""
            min_voltage = getattr(mw, "anode_min_voltage", None)
            min_time = getattr(mw, "anode_min_time", None)
            return (
                f"最小阳极值={float(min_anode):.6g}, "
                f"对应电压={float(min_voltage):.1f}V, 时间={min_time}"
            )
        except Exception:
            return ""

    def _build_channel_summary_lines(self) -> list[str]:
        rows = list(getattr(self.mw, "recorded_data", []) or [])
        lines = []
        for label, value_idx, unit_idx in self.CHANNEL_SUMMARY_FIELDS:
            values = []
            unit = ""
            for row in rows:
                if value_idx >= len(row):
                    continue
                try:
                    numeric = float(row[value_idx])
                except Exception:
                    continue
                if math.isnan(numeric):
                    continue
                values.append(numeric)
                if unit_idx is not None and unit_idx < len(row):
                    text = str(row[unit_idx] or "").strip()
                    if text:
                        unit = text
            if not values:
                continue
            average = sum(values) / len(values)
            suffix = f" {unit}" if unit else ""
            lines.append(
                f"{label}[最小/平均/最大]={min(values):.6g}/{average:.6g}/{max(values):.6g}{suffix}"
            )
        return lines

    def _resolve_data_path(self) -> str:
        try:
            path = str(self.mw.get_record_file_path() or "").strip()
        except Exception:
            path = ""
        return path or "未配置"
