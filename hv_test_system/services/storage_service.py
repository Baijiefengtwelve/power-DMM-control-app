from __future__ import annotations

import os


class StorageService:
    """Own record-path and local storage path helpers shared by GUI, web, and recording flows."""

    RECORD_PATH_PLACEHOLDERS = {
        "",
        "???????",
        "未选择保存路径",
        "<not selected>",
    }

    def __init__(self, mw):
        self.mw = mw

    def get_record_file_path(self) -> str:
        text = ""
        try:
            text = str(self.mw.path_label.text() or "").strip()
        except Exception:
            text = str(getattr(self.mw, "save_path", "") or "").strip()
        if text in self.RECORD_PATH_PLACEHOLDERS:
            return ""
        return text

    def has_record_file_path(self) -> bool:
        return bool(self.get_record_file_path())

    def set_record_file_path(self, path: str):
        clean_path = str(path or "").strip()
        self.mw.save_path = clean_path
        try:
            if hasattr(self.mw, "path_label"):
                self.mw.path_label.setText(clean_path or "<not selected>")
        except Exception:
            pass
        return clean_path

    def get_record_output_dir(self) -> str:
        path = self.get_record_file_path()
        if not path:
            return ""
        if os.path.isdir(path):
            return path
        return os.path.dirname(path)

    def resolve_record_download_path(self, filename: str) -> str:
        safe_name = os.path.basename(str(filename or ""))
        if not safe_name:
            return ""
        base_dir = self.get_record_output_dir()
        if not base_dir:
            return ""
        return os.path.join(base_dir, safe_name)

    def get_sqlite_db_path(self) -> str:
        try:
            return str(self.mw.sqlite_recorder.cfg.path)
        except Exception:
            return os.path.join("data", "session.sqlite")
