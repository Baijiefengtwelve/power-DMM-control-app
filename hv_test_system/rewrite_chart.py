# Rewrite chart_panel.py to support split layouts and filters
python_code = """from __future__ import annotations

from .common import *
from .utils import ScientificAxisItem


class ChartPanel(QWidget):
    \"\"\"Main chart-side panel for live telemetry plots with subplots and filters.\"\"\"

    TITLE = "\u5b9e\u65f6\u6570\u636e\u76d1\u6d4b"

    def __init__(self, main_window):
        super().__init__()
        self.main_window = main_window
        self.setup_ui()

    def setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(6, 6, 6, 6)
        layout.setSpacing(6)

        layout.addWidget(self._create_chart_title())
        layout.addLayout(self._create_filter_bar())
        
        self.main_window.plot_widget = pg.GraphicsLayoutWidget()
        self.main_window.plot_widget.setBackground("#FFFFFF")
        
        self.plot_top = self.main_window.plot_widget.addPlot(row=0, col=0)
        self.plot_top.showGrid(x=True, y=True, alpha=0.3)
        self.plot_top.setLabel("left", "\u7535\u6d41 (\u03bcA/mA)", color="#2C3E50", size="10pt")
        self.plot_top.getAxis("left").setPen(pg.mkPen(color="#2C3E50", width=1))
        self.plot_top.getAxis("bottom").setPen(pg.mkPen(color="#2C3E50", width=1))
        self.plot_top.addLegend(offset=(5, 5), verSpacing=-5, labelTextSize="8pt")
        
        self._configure_plot_performance(self.plot_top)
        
        self.plot_bottom = self.main_window.plot_widget.addPlot(row=1, col=0)
        self.plot_bottom.showGrid(x=True, y=True, alpha=0.3)
        self.plot_bottom.setLabel("left", "\u7535\u538b / \u6bd4\u7387", color="#2C3E50", size="10pt")
        self.plot_bottom.setLabel("bottom", "\u65f6\u95f4 (s)", color="#2C3E50", size="10pt")
        self.plot_bottom.getAxis("left").setPen(pg.mkPen(color="#2C3E50", width=1))
        self.plot_bottom.getAxis("bottom").setPen(pg.mkPen(color="#2C3E50", width=1))
        self.plot_bottom.addLegend(offset=(5, 5), verSpacing=-5, labelTextSize="8pt")
        
        # Replace normal right axis with scientific axis item specifically for vacuum
        sci_axis = ScientificAxisItem(orientation="right")
        self.plot_bottom.layout.removeItem(self.plot_bottom.getAxis("right"))
        self.plot_bottom.layout.addItem(sci_axis, 2, 2)
        self.plot_bottom.axes["right"] = {"item": sci_axis, "pos": (2, 2)}
        
        self._configure_plot_performance(self.plot_bottom)
        
        self.plot_bottom.setXLink(self.plot_top)

        self.main_window.plots = {}
        self._setup_vacuum_axis(self.plot_bottom)
        self._create_plot_items()
        self._apply_saved_plot_colors()
        
        layout.addWidget(self.main_window.plot_widget)

    def _create_chart_title(self):
        chart_title = QLabel(self.TITLE)
        chart_title.setObjectName("chartTitle")
        chart_title.setAlignment(Qt.AlignCenter)
        chart_title.setMinimumHeight(30)
        return chart_title

    def _create_filter_bar(self):
        hbox = QHBoxLayout()
        hbox.addWidget(QLabel("\u663e\u793a\u56fe\u7ebf:"))
        
        self.filters = {}
        lines = [
            ("cathode", "\u9634\u6781(uA)"),
            ("gate", "\u6805\u6781(uA)"),
            ("anode", "\u9633\u6781(uA)"),
            ("backup", "\u6536\u96c6\u6781(uA)"),
            ("gate_plus_anode", "\u6805+\u9633+\u6536"),
            ("keithley_voltage", "\u6805\u6781\u7535\u538b(V)"),
            ("anode_cathode_ratio", "\u7535\u6d41\u6bd4(%)"),
            ("vacuum", "\u771f\u7a7a(Pa)"),
        ]
        
        for key, name in lines:
            chk = QCheckBox(name)
            chk.setChecked(True)
            chk.toggled.connect(lambda checked, k=key: self._toggle_line(k, checked))
            hbox.addWidget(chk)
            self.filters[key] = chk
            
        hbox.addStretch()
        return hbox

    def _toggle_line(self, key, checked):
        if key in self.main_window.plots:
            self.main_window.plots[key].setVisible(checked)

    def _configure_plot_performance(self, plot_item):
        try:
            plot_item.setClipToView(True)
            plot_item.setDownsampling(auto=True, mode="peak")
        except TypeError:
            try:
                plot_item.setDownsampling(auto=True)
            except Exception:
                pass
        except Exception:
            pass

    def _setup_vacuum_axis(self, plot_item):
        try:
            plot_item.showAxis("right")
            plot_item.setLabel("right", "\u771f\u7a7a", units="Pa", color="#2C3E50", size="10pt")
            try:
                plot_item.getAxis("right").enableAutoSIPrefix(False)
            except Exception:
                pass

            self.main_window._vacuum_vb = pg.ViewBox()
            plot_item.scene().addItem(self.main_window._vacuum_vb)
            self.main_window._vacuum_vb.setXLink(plot_item.getViewBox())
            plot_item.getAxis("right").linkToView(self.main_window._vacuum_vb)

            def _update_views():
                self.main_window._vacuum_vb.setGeometry(plot_item.getViewBox().sceneBoundingRect())
                self.main_window._vacuum_vb.linkedViewChanged(
                    plot_item.getViewBox(),
                    self.main_window._vacuum_vb.XAxis,
                )

            plot_item.getViewBox().sigResized.connect(_update_views)
            _update_views()
        except Exception:
            pass

    def _plot_pen(self, key, fallback, line_width=1.5):
        return pg.mkPen(color=self.main_window.get_plot_color(key, fallback), width=line_width)

    def _create_plot_items(self):
        self.main_window.plots["cathode"] = self.plot_top.plot(
            pen=self._plot_pen("cathode", "#E74C3C"),
            name="\u9634\u6781",
        )
        self.main_window.plots["gate"] = self.plot_top.plot(
            pen=self._plot_pen("gate", "#2ECC71"),
            name="\u6805\u6781",
        )
        self.main_window.plots["anode"] = self.plot_top.plot(
            pen=self._plot_pen("anode", "#3498DB"),
            name="\u9633\u6781",
        )
        self.main_window.plots["backup"] = self.plot_top.plot(
            pen=self._plot_pen("backup", "#F39C12"),
            name="\u6536\u96c6\u6781",
        )
        self.main_window.plots["gate_plus_anode"] = self.plot_top.plot(
            pen=self._plot_pen("gate_plus_anode", "#E67E22"),
            name="\u6805\u6781+\u9633\u6781+\u6536\u96c6\u6781",
        )
        
        self.main_window.plots["keithley_voltage"] = self.plot_bottom.plot(
            pen=self._plot_pen("keithley_voltage", "#9B59B6"),
            name="\u6805\u6781\u7535\u538b",
        )
        self.main_window.plots["anode_cathode_ratio"] = self.plot_bottom.plot(
            pen=self._plot_pen("anode_cathode_ratio", "#1ABC9C"),
            name="(\u9633\u6781/\u9634\u6781)\u00d7100",
        )
        
        self._configure_plot_items_for_speed()
        self._create_vacuum_plot_item(self.plot_bottom)

    def _create_vacuum_plot_item(self, plot_item):
        try:
            vac_item = pg.PlotDataItem(
                pen=self._plot_pen("vacuum", "#7F8C8D"),
                name="\u771f\u7a7a",
            )
            self._configure_item_for_speed(vac_item)
            if hasattr(self.main_window, "_vacuum_vb"):
                self.main_window._vacuum_vb.addItem(vac_item)
            else:
                plot_item.addItem(vac_item)
            self.main_window.plots["vacuum"] = vac_item
            try:
                if plot_item.legend is not None:
                    plot_item.legend.addItem(vac_item, "\u771f\u7a7a")
            except Exception:
                pass
        except Exception:
            pass

    def _apply_saved_plot_colors(self):
        try:
            if hasattr(self.main_window, "apply_plot_colors"):
                self.main_window.apply_plot_colors()
        except Exception:
            pass

    def _configure_plot_items_for_speed(self):
        for item in getattr(self.main_window, "plots", {}).values():
            self._configure_item_for_speed(item)

    def _configure_item_for_speed(self, item):
        try:
            item.setClipToView(True)
        except Exception:
            pass
        try:
            item.setDownsampling(auto=True, method="peak")
        except TypeError:
            try:
                item.setDownsampling(auto=True)
            except Exception:
                pass
        except Exception:
            pass
        try:
            item.setSkipFiniteCheck(True)
        except Exception:
            pass
"""

with open('e:/application/hv_test_system_project_v2.10.3/hv_test_system/chart_panel.py', 'w', encoding='utf-8') as f:
    f.write(python_code)
