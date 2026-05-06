from __future__ import annotations

from .bootstrap_service import BootstrapService
from .config_sync_service import ConfigSyncService
from .display_service import DisplayService
from .device_manager import DeviceManager
from .device_shutdown_service import DeviceShutdownService
from .dialog_service import DialogService
from .feedback_service import FeedbackService
from .hv_worker_service import HVWorkerService
from .lifecycle_service import LifecycleService
from .maintenance_service import MaintenanceService
from .manual_control_service import ManualControlService
from .menu_service import MenuService
from .meter_connection_service import MeterConnectionService
from .meter_data_service import MeterDataService
from .panel_state_service import PanelStateService
from .plot_service import PlotService
from .power_catalog_service import PowerCatalogService
from .power_connection_service import PowerConnectionService
from .power_inventory_service import PowerInventoryService
from .power_panel_service import PowerPanelService
from .power_polling_service import PowerPollingService
from .port_refresh_service import PortRefreshService
from .recording_service import RecordingService
from .remote_command_service import RemoteCommandService
from .settings_service import SettingsService
from .stabilization_service import StabilizationService
from .state_snapshot_service import StateSnapshotService
from .storage_service import StorageService
from .test_control_service import TestControlService
from .test_runtime_service import TestRuntimeService
from .test_service import TestService
from .timer_service import TimerService
from .window_ui_service import WindowUIService


class ServiceRegistry:
    """Instantiate window-scoped services and wire their cross-service signals."""

    SERVICE_FACTORIES = (
        ("feedback_service", FeedbackService),
        ("dialog_service", DialogService),
        ("test_service", TestService),
        ("test_control_service", TestControlService),
        ("test_runtime_service", TestRuntimeService),
        ("stabilization_service", StabilizationService),
        ("power_catalog_service", PowerCatalogService),
        ("power_panel_service", PowerPanelService),
        ("power_inventory_service", PowerInventoryService),
        ("power_polling_service", PowerPollingService),
        ("display_service", DisplayService),
        ("plot_service", PlotService),
        ("lifecycle_service", LifecycleService),
        ("manual_control_service", ManualControlService),
        ("meter_connection_service", MeterConnectionService),
        ("meter_data_service", MeterDataService),
        ("timer_service", TimerService),
        ("device_manager", DeviceManager),
        ("device_shutdown_service", DeviceShutdownService),
        ("hv_worker_service", HVWorkerService),
        ("port_refresh_service", PortRefreshService),
        ("power_connection_service", PowerConnectionService),
        ("maintenance_service", MaintenanceService),
        ("panel_state_service", PanelStateService),
        ("settings_service", SettingsService),
        ("state_snapshot_service", StateSnapshotService),
        ("storage_service", StorageService),
        ("remote_command_service", RemoteCommandService),
        ("config_sync_service", ConfigSyncService),
        ("recording_service", RecordingService),
        ("menu_service", MenuService),
        ("window_ui_service", WindowUIService),
        ("bootstrap_service", BootstrapService),
    )

    def __init__(self, mw, service_factories=None):
        self.mw = mw
        self.service_factories = service_factories or self.SERVICE_FACTORIES

    def register_services(self):
        for attr_name, factory in self.service_factories:
            setattr(self.mw, attr_name, factory(self.mw))

        self._connect_signals()
        return self

    def _connect_signals(self):
        self.mw.test_service.log.connect(self.mw.log_message)
        self.mw.test_service.state_change.connect(self.mw._on_test_state_change)
        self.mw.test_service.finished.connect(self.mw._on_test_finished)
        self.mw.stabilization_service.log.connect(self.mw.log_message)
