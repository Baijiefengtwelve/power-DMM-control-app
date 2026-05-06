from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class RecordSample:
    time_text: str
    test_power_name: str
    test_power_voltage: float
    cathode: float
    cathode_unit: str
    gate: float
    gate_unit: str
    anode: float
    anode_unit: str
    backup: float
    backup_unit: str
    vacuum: float
    stabilization_power_name: str
    stabilization_power_voltage: float
    test_power_type: str = ""
    stabilization_power_type: str = ""
    session_id: str = ""
    run_id: str = ""
    is_testing: bool = False
    is_stabilizing: bool = False
    is_recording: bool = False
    timestamp_ns: int = 0

    @classmethod
    def from_measurements(
        cls,
        *,
        time_text: str,
        test_power_name: str,
        test_power_voltage: float,
        cathode: float,
        cathode_unit: str,
        gate: float,
        gate_unit: str,
        anode: float,
        anode_unit: str,
        backup: float,
        backup_unit: str,
        vacuum: float,
        stabilization_power_name: str,
        stabilization_power_voltage: float,
        test_power_type: str = "",
        stabilization_power_type: str = "",
        session_id: str = "",
        run_id: str = "",
        is_testing: bool = False,
        is_stabilizing: bool = False,
        is_recording: bool = False,
        timestamp_ns: int = 0,
    ) -> "RecordSample":
        return cls(
            time_text=str(time_text),
            test_power_name=str(test_power_name),
            test_power_voltage=float(test_power_voltage),
            cathode=float(cathode),
            cathode_unit=str(cathode_unit or ""),
            gate=float(gate),
            gate_unit=str(gate_unit or ""),
            anode=float(anode),
            anode_unit=str(anode_unit or ""),
            backup=float(backup),
            backup_unit=str(backup_unit or ""),
            vacuum=float(vacuum),
            stabilization_power_name=str(stabilization_power_name),
            stabilization_power_voltage=float(stabilization_power_voltage),
            test_power_type=str(test_power_type),
            stabilization_power_type=str(stabilization_power_type),
            session_id=str(session_id),
            run_id=str(run_id),
            is_testing=bool(is_testing),
            is_stabilizing=bool(is_stabilizing),
            is_recording=bool(is_recording),
            timestamp_ns=int(timestamp_ns),
        )

    @property
    def gate_plus_anode_unit(self) -> str:
        units = {self.gate_unit, self.anode_unit, self.backup_unit}
        units.discard("")
        return next(iter(units)) if len(units) == 1 else ""

    @property
    def gate_plus_anode(self) -> float:
        if not self.gate_plus_anode_unit and any((self.gate_unit, self.anode_unit, self.backup_unit)):
            return math.nan
        return self.gate + self.anode + self.backup

    @property
    def anode_cathode_ratio(self) -> float:
        if self.cathode_unit and self.anode_unit and self.cathode_unit != self.anode_unit:
            return math.nan
        if self.cathode == 0:
            return math.nan
        return (self.anode / self.cathode) * 100.0

    @property
    def timestamp_ms(self) -> int:
        return int(self.timestamp_ns / 1_000_000) if self.timestamp_ns else 0

    def to_csv_row(self) -> list:
        return [
            self.time_text,
            self.test_power_name,
            round(self.test_power_voltage, 2),
            round(self.cathode, 4),
            self.cathode_unit,
            round(self.gate, 4),
            self.gate_unit,
            round(self.anode, 4),
            self.anode_unit,
            round(self.backup, 4),
            self.backup_unit,
            self.vacuum,
            self.stabilization_power_name,
            round(self.stabilization_power_voltage, 2),
            round(self.gate_plus_anode, 4) if self.gate_plus_anode == self.gate_plus_anode else "",
            self.gate_plus_anode_unit,
            round(self.anode_cathode_ratio, 2) if self.anode_cathode_ratio == self.anode_cathode_ratio else "",
        ]

    def to_sqlite_row(self) -> dict[str, float | str]:
        return {
            "time_text": self.time_text,
            "test_power_name": self.test_power_name,
            "stabilization_power_name": self.stabilization_power_name,
            "hv_voltage": self.test_power_voltage,
            "cathode": self.cathode,
            "cathode_unit": self.cathode_unit,
            "gate": self.gate,
            "gate_unit": self.gate_unit,
            "anode": self.anode,
            "anode_unit": self.anode_unit,
            "backup": self.backup,
            "backup_unit": self.backup_unit,
            "vacuum": self.vacuum,
            "keithley_voltage": self.stabilization_power_voltage,
            "gate_plus_anode": self.gate_plus_anode,
            "gate_plus_anode_unit": self.gate_plus_anode_unit,
            "anode_cathode_ratio": self.anode_cathode_ratio,
        }

    def to_influx_fields(self) -> dict[str, float | bool | str]:
        return {
            "cathode": self.cathode,
            "cathode_unit": self.cathode_unit,
            "gate": self.gate,
            "gate_unit": self.gate_unit,
            "anode": self.anode,
            "anode_unit": self.anode_unit,
            "backup": self.backup,
            "backup_unit": self.backup_unit,
            "vacuum": self.vacuum,
            "hv_vout": self.test_power_voltage,
            "keithley_voltage": self.stabilization_power_voltage,
            "gate_plus_anode": self.gate_plus_anode,
            "gate_plus_anode_unit": self.gate_plus_anode_unit,
            "anode_cathode_ratio": self.anode_cathode_ratio,
            "is_testing": self.is_testing,
            "is_stabilizing": self.is_stabilizing,
            "is_recording": self.is_recording,
        }

    def to_influx_tags(self) -> dict[str, str]:
        return {
            "test_power_name": self.test_power_name,
            "test_power_type": self.test_power_type,
            "stab_power_name": self.stabilization_power_name,
            "stab_power_type": self.stabilization_power_type,
            "session": self.session_id,
            "run": self.run_id,
        }
