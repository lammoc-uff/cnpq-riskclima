import logging
from collections.abc import Callable
from pathlib import Path

import xarray as xr

from riskclima_xhwi.config.settings import CalibrationPolicy, CMIP6Settings, XHWISettings

LOGGER = logging.getLogger(__name__)


def require_persistent_calibration(settings: XHWISettings) -> None:
    """Reject in-memory policy for commands whose purpose is writing a calibration file."""
    if settings.calibration_policy is CalibrationPolicy.IN_MEMORY:
        raise ValueError(
            "make-calibration cannot use calibration_policy=in_memory; choose a file policy"
        )


def resolve_calibration(
    settings: XHWISettings,
    create: Callable[[], Path],
) -> Path | None:
    """Resolve calibration storage according to the source policy.

    Parameters
    ----------
    settings
        Source settings containing the calibration policy and path.
    create
        Function that creates or replaces the configured calibration file.

    Returns
    -------
    pathlib.Path or None
        Valid saved calibration, or ``None`` for in-memory calibration.
    """
    path = settings.calibration_output
    if settings.calibration_policy is CalibrationPolicy.IN_MEMORY:
        return None
    if settings.calibration_policy is CalibrationPolicy.REBUILD:
        created = create()
        validate_calibration_file(created, settings)
        return created
    if path.exists():
        validate_calibration_file(path, settings)
        return path
    if settings.calibration_policy is CalibrationPolicy.CREATE_IF_MISSING:
        created = create()
        validate_calibration_file(created, settings)
        return created
    raise FileNotFoundError(f"Required calibration file does not exist: {path}")


def validate_calibration_file(path: Path, settings: XHWISettings) -> None:
    """Validate lightweight dimensions and available calibration identity metadata."""
    with xr.open_dataarray(path, engine=settings.netcdf_engine) as calibration:
        dimensions = set(calibration.dims)
        if "time" in dimensions:
            dimensions.remove("time")
            dimensions.add("calibration_time")
        required = {"calibration_time", "lat", "lon"}
        if not required.issubset(dimensions):
            raise ValueError(
                "Saved calibration must have calibration_time, lat, and lon dimensions."
            )
        configured_period = f"{settings.calibration_start} to {settings.calibration_end}"
        stored_period = calibration.attrs.get("calibration_period")
        if stored_period is None:
            raise ValueError("Saved calibration must define calibration_period metadata.")
        if stored_period != configured_period:
            raise ValueError(
                f"Calibration period mismatch: expected {configured_period}, got {stored_period}."
            )
        if not isinstance(settings, CMIP6Settings):
            return
        stored_grid = calibration.attrs.get("grid_label")
        if stored_grid is None:
            raise ValueError("CMIP6 calibration must define grid_label metadata.")
        if stored_grid != settings.grid:
            raise ValueError(
                f"Calibration grid mismatch: expected {settings.grid}, got {stored_grid}."
            )
        stored_model = calibration.attrs.get("model_id")
        if stored_model is None:
            raise ValueError("CMIP6 calibration must define model_id metadata.")
        if stored_model != settings.model:
            raise ValueError(
                f"Calibration model mismatch: expected {settings.model}, got {stored_model}."
            )
