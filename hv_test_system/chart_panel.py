from __future__ import annotations

from .common import *
from .utils import ScientificAxisItem


class ChartPanel(QWidget):
    """Main chart-side panel for live telemetry plots."""

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
        self.main_window.plot_widget = self._create_plot_widget()
        self.main_window.plots = {}
        self._setup_vacuum_axis(self.main_window.plot_widget)
        self._create_plot_items(self.main_window.plot_widget)
        self._apply_saved_plot_colors()
        layout.addWidget(self.main_window.plot_widget)

    def _create_chart_title(self):
        chart_title = QLabel(self.TITLE)
        chart_title.setObjectName("chartTitle")
        chart_title.setAlignment(Qt.AlignCenter)
        chart_title.setMinimumHeight(30)
        return chart_title

    def _create_plot_widget(self):
        plot_widget = pg.PlotWidget(axisItems={"right": ScientificAxisItem(orientation="right")})
        self._configure_plot_axes(plot_widget)
        self._configure_plot_performance(plot_widget)
        plot_widget.addLegend(offset=(5, 5), verSpacing=-5, labelTextSize="8pt")
        return plot_widget

    def _configure_plot_axes(self, plot_widget):
        plot_widget.setBackground("#FFFFFF")
        plot_widget.showGrid(x=True, y=True, alpha=0.3)
        plot_widget.setLabel("left", "\u6570\u503c (mV / uA)", color="#2C3E50", size="10pt")
        plot_widget.setLabel("bottom", "\u65f6\u95f4 (s)", color="#2C3E50", size="10pt")
        plot_widget.getAxis("left").setPen(pg.mkPen(color="#2C3E50", width=1))
        plot_widget.getAxis("bottom").setPen(pg.mkPen(color="#2C3E50", width=1))

    def _configure_plot_performance(self, plot_widget):
        plot_widget.setClipToView(True)
        plot_widget.setDownsampling(auto=True, mode="peak")
        try:
            plot_widget.setAntialiasing(False)
        except Exception:
            pass

        plot_item = plot_widget.getPlotItem()
        try:
            plot_item.setClipToView(True)
        except Exception:
            pass
        try:
            plot_item.setDownsampling(auto=True, mode="peak")
        except TypeError:
            try:
                plot_item.setDownsampling(auto=True)
            except Exception:
                pass
        except Exception:
            pass

    def _setup_vacuum_axis(self, plot_widget):
        try:
            plot_widget.showAxis("right")
            plot_widget.setLabel("right", "\u771f\u7a7a", units="Pa", color="#2C3E50", size="10pt")
            try:
                plot_widget.getAxis("right").enableAutoSIPrefix(False)
            except Exception:
                pass

            self.main_window._vacuum_vb = pg.ViewBox()
            plot_widget.scene().addItem(self.main_window._vacuum_vb)
            self.main_window._vacuum_vb.setXLink(plot_widget.getViewBox())
            plot_widget.getAxis("right").linkToView(self.main_window._vacuum_vb)

            def _update_views():
                self.main_window._vacuum_vb.setGeometry(plot_widget.getViewBox().sceneBoundingRect())
                self.main_window._vacuum_vb.linkedViewChanged(
                    plot_widget.getViewBox(),
                    self.main_window._vacuum_vb.XAxis,
                )

            plot_widget.getViewBox().sigResized.connect(_update_views)
            _update_views()
        except Exception:
            pass

    def _plot_pen(self, key, fallback, line_width=1.5):
        return pg.mkPen(color=self.main_window.get_plot_color(key, fallback), width=line_width)

    def _create_plot_items(self, plot_widget):
        self.main_window.plots["cathode"] = plot_widget.plot(
            pen=self._plot_pen("cathode", "#E74C3C"),
            name="\u9634\u6781",
        )
        self.main_window.plots["gate"] = plot_widget.plot(
            pen=self._plot_pen("gate", "#2ECC71"),
            name="\u6805\u6781",
        )
        self.main_window.plots["anode"] = plot_widget.plot(
            pen=self._plot_pen("anode", "#3498DB"),
            name="\u9633\u6781",
        )
        self.main_window.plots["backup"] = plot_widget.plot(
            pen=self._plot_pen("backup", "#F39C12"),
            name="\u6536\u96c6\u6781",
        )
        self.main_window.plots["keithley_voltage"] = plot_widget.plot(
            pen=self._plot_pen("keithley_voltage", "#9B59B6"),
            name="\u7a33\u6d41\u7535\u6e90\u7535\u538b",
        )
        self.main_window.plots["gate_plus_anode"] = plot_widget.plot(
            pen=self._plot_pen("gate_plus_anode", "#E67E22"),
            name="\u6805\u6781+\u9633\u6781+\u6536\u96c6\u6781",
        )
        self.main_window.plots["anode_cathode_ratio"] = plot_widget.plot(
            pen=self._plot_pen("anode_cathode_ratio", "#1ABC9C"),
            name="(\u9633\u6781/\u9634\u6781)\u00d7100",
        )
        self._configure_plot_items_for_speed()
        self._create_vacuum_plot_item(plot_widget)

    def _create_vacuum_plot_item(self, plot_widget):
        try:
            vac_item = pg.PlotDataItem(
                pen=self._plot_pen("vacuum", "#7F8C8D"),
                name="\u771f\u7a7a",
            )
            self._configure_item_for_speed(vac_item)
            if hasattr(self.main_window, "_vacuum_vb"):
                self.main_window._vacuum_vb.addItem(vac_item)
            else:
                plot_widget.addItem(vac_item)
            self.main_window.plots["vacuum"] = vac_item
            try:
                if plot_widget.legend is not None:
                    plot_widget.legend.addItem(vac_item, "\u771f\u7a7a")
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
