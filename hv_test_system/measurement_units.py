from __future__ import annotations

from dataclasses import dataclass


STANDARD_VOLTAGE_UNIT = "mV"
STANDARD_CURRENT_UNIT = "uA"
VACUUM_UNIT = "Pa"

_VOLTAGE_SCALES = {
    "v": 1000.0,
    "mv": 1.0,
}

_CURRENT_SCALES = {
    "a": 1_000_000.0,
    "ma": 1000.0,
    "ua": 1.0,
    "μa": 1.0,
    "µa": 1.0,
}


@dataclass(frozen=True)
class StandardMeasurement:
    value: float
    unit: str
    kind: str


def normalize_unit_text(unit: str | None) -> str:
    text = str(unit or "").strip()
    return text.replace("μ", "u").replace("µ", "u")


def infer_measurement_kind(kind: str | None, unit: str | None) -> str:
    kind_text = str(kind or "").strip().lower()
    if kind_text in {"voltage", "current", "vacuum"}:
        return kind_text

    unit_text = normalize_unit_text(unit).lower()
    if unit_text in _VOLTAGE_SCALES:
        return "voltage"
    if unit_text in _CURRENT_SCALES:
        return "current"
    if unit_text == "pa":
        return "vacuum"
    return ""


def is_supported_meter_kind(kind: str | None, unit: str | None = None) -> bool:
    return infer_measurement_kind(kind, unit) in {"voltage", "current"}


def standard_unit_for_kind(kind: str) -> str:
    normalized = infer_measurement_kind(kind, "")
    if normalized == "voltage":
        return STANDARD_VOLTAGE_UNIT
    if normalized == "current":
        return STANDARD_CURRENT_UNIT
    if normalized == "vacuum":
        return VACUUM_UNIT
    return ""


def normalize_meter_measurement(value, unit: str | None, kind: str | None = None) -> StandardMeasurement:
    numeric_value = float(value)
    unit_text = normalize_unit_text(unit)
    kind_text = infer_measurement_kind(kind, unit_text)

    if kind_text == "voltage":
        scale = _VOLTAGE_SCALES.get(unit_text.lower())
        if scale is None:
            raise ValueError(f"Unsupported voltage unit: {unit_text}")
        return StandardMeasurement(numeric_value * scale, STANDARD_VOLTAGE_UNIT, "voltage")

    if kind_text == "current":
        scale = _CURRENT_SCALES.get(unit_text.lower())
        if scale is None:
            raise ValueError(f"Unsupported current unit: {unit_text}")
        return StandardMeasurement(numeric_value * scale, STANDARD_CURRENT_UNIT, "current")

    raise ValueError(f"Unsupported meter measurement: kind={kind!r}, unit={unit!r}")


def format_measurement_value(value, unit: str | None) -> str:
    numeric_value = float(value)
    clean_unit = str(unit or "").strip()
    if clean_unit == VACUUM_UNIT:
        return f"{numeric_value:.3e} {clean_unit}"
    if abs(numeric_value) >= 10000:
        return f"{numeric_value:.1f} {clean_unit}"
    if abs(numeric_value) >= 1000:
        return f"{numeric_value:.2f} {clean_unit}"
    return f"{numeric_value:.3f} {clean_unit}"

