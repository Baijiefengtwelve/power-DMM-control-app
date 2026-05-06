from __future__ import annotations

import json
import os
from datetime import datetime

from PyQt5.QtWidgets import QDialog, QInputDialog, QMessageBox

from ..settings_dialog_models import (
    StabilizationSettingsDialogState,
    TestSettingsDialogState,
)
from ..ui_dialogs import CurrentStabilizationDialog, TestSettingsDialog


class SettingsService:
    """Handle settings dialogs, validation, and summary generation."""

    PRESET_DIRNAME = "presets"
    PRESET_VERSION = 1
    CURRENT_SOURCE_LABELS = {
        "keithley": "Keithley",
        "cathode": "阴极",
        "gate": "栅极",
        "anode": "阳极",
        "backup": "收集极",
    }
    ALGORITHM_LABELS = {
        "pid": "PID",
        "approach": "接近算法",
    }

    def __init__(self, mw):
        self.mw = mw

    def current_source_combo_index(self, current_source: str) -> int:
        return CurrentStabilizationDialog.current_source_combo_index(current_source)

    def combo_index_current_source(self, index: int) -> str:
        return CurrentStabilizationDialog.combo_index_current_source(index)

    def _create_test_dialog(self):
        return TestSettingsDialog(self.mw)

    def _create_stabilization_dialog(self):
        return CurrentStabilizationDialog(self.mw)

    def _power_source_names(self) -> list[str]:
        return list(self.mw.list_power_source_names(include_auto=True))

    def _show_warning(self, message: str):
        text = str(message or "").strip()
        if not text:
            return
        try:
            QMessageBox.warning(self.mw, "设置冲突", text)
        except Exception:
            pass
        try:
            self.mw.log_message(text)
        except Exception:
            pass

    def _show_info(self, message: str):
        text = str(message or "").strip()
        if not text:
            return
        try:
            self.mw.show_status_message(text, timeout_ms=3000)
        except Exception:
            pass
        try:
            self.mw.log_message(text)
        except Exception:
            pass

    def _preset_root(self) -> str:
        config_manager = getattr(self.mw, "config_manager", None)
        config_path = getattr(config_manager, "config_file", "config.ini")
        root = os.path.join(
            os.path.dirname(os.path.abspath(str(config_path))),
            self.PRESET_DIRNAME,
        )
        os.makedirs(root, exist_ok=True)
        return root

    def _preset_file_path(self, preset_name: str) -> str:
        safe_name = "".join(ch if ch not in '<>:"/\\|?*' else "_" for ch in str(preset_name or "").strip())
        safe_name = safe_name.strip(". ") or "preset"
        return os.path.join(self._preset_root(), f"{safe_name}.json")

    def _available_preset_names(self) -> list[str]:
        root = self._preset_root()
        names = []
        try:
            for entry in os.listdir(root):
                if not entry.lower().endswith(".json"):
                    continue
                names.append(os.path.splitext(entry)[0])
        except Exception:
            return []
        return sorted(set(names))

    def _prompt_preset_name(self, title: str, label: str) -> str | None:
        try:
            name, accepted = QInputDialog.getText(self.mw, title, label)
        except Exception as exc:
            self.mw.log_message(f"打开预设命名对话框失败: {exc}")
            return None
        if not accepted:
            return None
        normalized = str(name or "").strip()
        return normalized or None

    def _prompt_existing_preset_name(self, title: str, label: str) -> str | None:
        names = self._available_preset_names()
        if not names:
            self._show_info("当前还没有可加载的预设")
            return None
        try:
            name, accepted = QInputDialog.getItem(self.mw, title, label, names, 0, False)
        except Exception as exc:
            self.mw.log_message(f"打开预设选择对话框失败: {exc}")
            return None
        if not accepted:
            return None
        normalized = str(name or "").strip()
        return normalized or None

    def _build_preset_payload(
        self,
        *,
        test_state: TestSettingsDialogState | None = None,
        stabilization_state: StabilizationSettingsDialogState | None = None,
    ) -> dict:
        test_state = test_state or self.build_test_dialog_state()
        stabilization_state = stabilization_state or self.build_stabilization_dialog_state()
        return {
            "version": self.PRESET_VERSION,
            "saved_at": datetime.now().isoformat(timespec="seconds"),
            "test": test_state.to_param_updates(),
            "stabilization": stabilization_state.to_param_updates(),
        }

    def _write_preset_payload(self, preset_name: str, payload: dict):
        preset_path = self._preset_file_path(preset_name)
        with open(preset_path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
        return preset_path

    def _read_preset_payload(self, preset_name: str) -> dict | None:
        preset_path = self._preset_file_path(preset_name)
        try:
            with open(preset_path, "r", encoding="utf-8") as fh:
                payload = json.load(fh)
        except Exception as exc:
            self.mw.log_message(f"读取预设失败: {exc}")
            return None
        if not isinstance(payload, dict):
            self.mw.log_message("读取预设失败: 文件格式无效")
            return None
        return payload

    def save_test_preset_from_dialog(self, state: TestSettingsDialogState) -> bool:
        preset_name = self._prompt_preset_name("保存测试预设", "请输入预设名称:")
        if preset_name is None:
            return False
        try:
            payload = self._build_preset_payload(
                test_state=state,
                stabilization_state=self.build_stabilization_dialog_state(),
            )
            preset_path = self._write_preset_payload(preset_name, payload)
            self._show_info(f"测试预设已保存: {preset_name} ({preset_path})")
            return True
        except Exception as exc:
            self.mw.log_message(f"保存测试预设失败: {exc}")
            return False

    def save_stabilization_preset_from_dialog(self, state: StabilizationSettingsDialogState) -> bool:
        preset_name = self._prompt_preset_name("保存稳流预设", "请输入预设名称:")
        if preset_name is None:
            return False
        try:
            payload = self._build_preset_payload(
                test_state=self.build_test_dialog_state(),
                stabilization_state=state,
            )
            preset_path = self._write_preset_payload(preset_name, payload)
            self._show_info(f"稳流预设已保存: {preset_name} ({preset_path})")
            return True
        except Exception as exc:
            self.mw.log_message(f"保存稳流预设失败: {exc}")
            return False

    def load_test_preset_for_dialog(self) -> TestSettingsDialogState | None:
        preset_name = self._prompt_existing_preset_name("加载测试预设", "请选择预设:")
        if preset_name is None:
            return None
        payload = self._read_preset_payload(preset_name)
        if payload is None:
            return None
        try:
            state = TestSettingsDialogState.from_params(payload.get("test", {}))
        except Exception as exc:
            self.mw.log_message(f"加载测试预设失败: {exc}")
            return None
        self._show_info(f"测试预设已加载: {preset_name}")
        return state

    def load_stabilization_preset_for_dialog(self) -> StabilizationSettingsDialogState | None:
        preset_name = self._prompt_existing_preset_name("加载稳流预设", "请选择预设:")
        if preset_name is None:
            return None
        payload = self._read_preset_payload(preset_name)
        if payload is None:
            return None
        try:
            state = StabilizationSettingsDialogState.from_params(payload.get("stabilization", {}))
        except Exception as exc:
            self.mw.log_message(f"加载稳流预设失败: {exc}")
            return None
        self._show_info(f"稳流预设已加载: {preset_name}")
        return state

    def _selected_power_interlock_error(
        self,
        *,
        test_source_name: str | None = None,
        stabilization_source_name: str | None = None,
    ) -> str:
        test_name = (
            self.mw._get_selected_power_name("test")
            if test_source_name is None
            else str(test_source_name or "").strip()
        )
        stab_name = (
            self.mw._get_selected_power_name("stabilization")
            if stabilization_source_name is None
            else str(stabilization_source_name or "").strip()
        )
        return self.mw.power_catalog_service.validate_selected_power_interlock(
            test_source_name=test_name,
            stabilization_source_name=stab_name,
        )

    def build_test_dialog_state(self) -> TestSettingsDialogState:
        return TestSettingsDialogState.from_params(
            self.mw.test_params,
            power_source_name=self.mw._get_selected_power_name("test"),
        )

    def build_stabilization_dialog_state(self) -> StabilizationSettingsDialogState:
        return StabilizationSettingsDialogState.from_params(
            self.mw.stabilization_params,
            power_source_name=self.mw._get_selected_power_name("stabilization"),
        )

    def apply_test_dialog_state(self, state: TestSettingsDialogState) -> bool:
        interlock_error = self._selected_power_interlock_error(test_source_name=state.power_source_name)
        if interlock_error:
            self._show_warning(interlock_error)
            return False

        try:
            self.mw.apply_test_params(state.to_param_updates())
            self._persist_parameter_updates()
            self.mw.log_message("测试设置已更新")
            return True
        except Exception as exc:
            self.mw.log_message(f"更新测试设置失败: {exc}")
            return False

    def apply_stabilization_dialog_state(self, state: StabilizationSettingsDialogState) -> bool:
        interlock_error = self._selected_power_interlock_error(
            stabilization_source_name=state.power_source_name
        )
        if interlock_error:
            self._show_warning(interlock_error)
            return False

        try:
            self.mw.apply_stabilization_params(state.to_param_updates())
            self._persist_parameter_updates()
            self.mw.log_message("稳流设置已更新")
            return True
        except Exception as exc:
            self.mw.log_message(f"更新稳流设置失败: {exc}")
            return False
    def _persist_parameter_updates(self):
        mw = self.mw
        self.update_settings_display()
        mw.update_power_action_buttons()
        mw.save_config_from_ui()

    def show_current_stabilization_settings(self):
        dialog = self._create_stabilization_dialog()
        dialog.set_power_source_names(self._power_source_names())
        dialog.apply_state(self.build_stabilization_dialog_state())

        if dialog.exec_() != QDialog.Accepted:
            return False
        return self.apply_stabilization_dialog_state(dialog.read_state())

    def show_test_settings(self):
        dialog = self._create_test_dialog()
        dialog.set_power_source_names(self._power_source_names())
        dialog.apply_state(self.build_test_dialog_state())

        if dialog.exec_() != QDialog.Accepted:
            return False
        return self.apply_test_dialog_state(dialog.read_state())

    def _fmt(self, value):
        try:
            return f"{float(value):g}"
        except Exception:
            return str(value)

    def _algorithm_label(self, value) -> str:
        key = str(value or "pid").strip().lower()
        return self.ALGORITHM_LABELS.get(key, str(value))

    def _feedback_label(self, value) -> str:
        key = str(value or "keithley").strip().lower()
        return self.CURRENT_SOURCE_LABELS.get(key, str(value))

    def build_test_summary(self) -> str:
        mw = self.mw
        test_source = mw._get_selected_power_name("test")
        text = (
            f"测试[电源={test_source}, 起始={self._fmt(mw.test_params['start_voltage'])}V, "
            f"目标={self._fmt(mw.test_params['target_voltage'])}V, "
            f"步进={self._fmt(mw.test_params['voltage_step'])}V, "
            f"延时={self._fmt(mw.test_params['step_delay'])}s"
        )
        try:
            cycle_time = float(mw.test_params.get("cycle_time", 0) or 0)
        except Exception:
            cycle_time = 0.0
        if cycle_time > 0:
            text += f", 循环={self._fmt(cycle_time)}s"
        return text + "]"

    def build_stabilization_summary(self) -> str:
        mw = self.mw
        stab_source = mw._get_selected_power_name("stabilization")
        params = mw.stabilization_params
        feedback = self._feedback_label(params.get("current_source", "keithley"))
        summary = (
            f"稳流[电源={stab_source}, 起始={self._fmt(mw.stabilization_params['start_voltage'])}V, "
            f"目标={self._fmt(mw.stabilization_params['target_current'])}(uA / mV), "
            f"范围={self._fmt(mw.stabilization_params['stability_range'])}(uA / mV), "
            f"反馈={feedback}, "
            f"频率={self._fmt(mw.stabilization_params['adjust_frequency'])}s, "
            f"最大步进={self._fmt(mw.stabilization_params['max_adjust_voltage'])}V, "
            f"算法={self._algorithm_label(mw.stabilization_params.get('algorithm', 'pid'))}]"
        )
        if str(params.get("algorithm", "pid")).strip().lower() == "pid":
            summary = summary[:-1] + (
                f", Kp={self._fmt(params.get('pid_kp', 0.05))}, "
                f"Ki={self._fmt(params.get('pid_ki', 0.01))}, "
                f"Kd={self._fmt(params.get('pid_kd', 0.0))}]"
            )
        return summary

    def build_settings_summary(self) -> str:
        return f"当前设置: {self.build_test_summary()}; {self.build_stabilization_summary()}"

    def update_settings_display(self) -> str:
        summary = self.build_settings_summary()
        try:
            self.mw.current_settings_label.setText(summary)
        except Exception:
            pass
        return summary

