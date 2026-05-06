from __future__ import annotations

from datetime import datetime

from PyQt5 import QtGui
from PyQt5.QtCore import QTimer


class FeedbackService:
    """Own lightweight user feedback widgets such as the status bar and log panel."""

    MAX_LOG_LINES = 1000
    TRIM_BATCH_LINES = 500

    def __init__(self, mw):
        self.mw = mw

    def show_status_message(self, message, timeout_ms: int = 0):
        text = str(message)
        status_bar = getattr(self.mw, "status_bar", None)
        if status_bar is None:
            return text

        try:
            if int(timeout_ms or 0) > 0:
                status_bar.showMessage(text, int(timeout_ms))
            else:
                status_bar.showMessage(text)
        except TypeError:
            status_bar.showMessage(text)
        except Exception:
            pass
        return text

    def log_message(self, message):
        text = str(message)
        formatted_message = f"[{datetime.now().strftime('%H:%M:%S')}] {text}"
        log_widget = getattr(self.mw, "log_text", None)
        if log_widget is None:
            return formatted_message

        try:
            scrollbar = self._safe_scrollbar(log_widget)
            old_value = scrollbar.value() if scrollbar is not None else None
            old_max = scrollbar.maximum() if scrollbar is not None else None

            self._append_log_line(log_widget, formatted_message)
            self._trim_log_widget(log_widget)

            should_restore = (
                scrollbar is not None
                and old_value is not None
                and old_max is not None
                and old_value < old_max
            )
            if should_restore:
                self._restore_scrollbar_position(scrollbar, old_value, old_max)
        except Exception as exc:
            print(f"Failed to append log message: {exc}")
        return formatted_message

    def _safe_scrollbar(self, log_widget):
        try:
            return log_widget.verticalScrollBar()
        except Exception:
            return None

    def _trim_log_widget(self, log_widget):
        try:
            if hasattr(log_widget, "setMaximumBlockCount"):
                return
            if log_widget.document().lineCount() <= self.MAX_LOG_LINES:
                return
            cursor = log_widget.textCursor()
            cursor.movePosition(QtGui.QTextCursor.Start)
            cursor.movePosition(
                QtGui.QTextCursor.Down,
                QtGui.QTextCursor.KeepAnchor,
                self.TRIM_BATCH_LINES,
            )
            cursor.removeSelectedText()
        except Exception:
            pass

    def _restore_scrollbar_position(self, scrollbar, old_value, old_max):
        def _restore():
            try:
                new_max = scrollbar.maximum()
                if old_max is None:
                    scrollbar.setValue(old_value)
                    return
                scrollbar.setValue(min(old_value, new_max))
            except Exception:
                pass

        QTimer.singleShot(0, _restore)

    def _append_log_line(self, log_widget, formatted_message: str):
        try:
            if hasattr(log_widget, "appendPlainText"):
                log_widget.appendPlainText(formatted_message)
                return
        except Exception:
            pass
        log_widget.append(formatted_message)
