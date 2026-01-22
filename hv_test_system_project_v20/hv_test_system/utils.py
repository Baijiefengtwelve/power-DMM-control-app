from __future__ import annotations

from .common import *

class ScientificAxisItem(pg.AxisItem):
    """AxisItem with scientific-notation tick labels."""
    def tickStrings(self, values, scale, spacing):
        # values are in data coordinates
        out = []
        for v in values:
            try:
                if v == 0 or (isinstance(v, (int, float)) and abs(v) < 1e-300):
                    out.append("0")
                else:
                    out.append(f"{float(v):.2e}")
            except Exception:
                out.append(str(v))
        return out

