import argparse
import logging
from collections.abc import Sequence
from pathlib import Path

from riskclima_xhwi.config.settings import ERA5Settings, ExistingFilePolicy
from riskclima_xhwi.io.writers import write_calibration_netcdf
from riskclima_xhwi.pipeline.data_access import (
    open_calibration_tasmax_from_t2m,
    open_t2m_calibration_inputs,
)
from riskclima_xhwi.pipeline.policies import require_persistent_calibration, resolve_calibration
from riskclima_xhwi.scripts.common import add_calibration_policy_argument, configure_logging
from riskclima_xhwi.scripts.era5.common import add_common_arguments, settings_from_args

LOGGER = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    """Build the ERA5 calibration parser."""
    parser = argparse.ArgumentParser(description="Build the configured ERA5 calibration.")
    add_common_arguments(parser)
    add_calibration_policy_argument(parser)
    return parser


def run(settings: ERA5Settings) -> Path:
    """Build and replace the configured ERA5 calibration file."""
    tas_c = open_t2m_calibration_inputs(settings.zarr_url, settings)
    calibration = open_calibration_tasmax_from_t2m(tas_c, settings)
    calibration.name = "tasmax_calibration"
    calibration.attrs.update(
        {
            "long_name": "Daily maximum 2 m temperature for XHWI calibration",
            "units": "degC",
            "source_variable": settings.variable_t2m,
            "calculation": "daily maximum from hourly ERA5 temperature",
            "calibration_period": f"{settings.calibration_start} to {settings.calibration_end}",
        }
    )
    written = write_calibration_netcdf(
        calibration,
        settings.calibration_output,
        settings=settings,
        policy=ExistingFilePolicy.OVERWRITE,
    )
    LOGGER.info("Calibration file written: %s", written)
    return written


def main(argv: Sequence[str] | None = None) -> None:
    """Run ERA5 calibration according to the configured policy."""
    configured = ERA5Settings()
    args = build_parser().parse_args(argv)
    settings = settings_from_args(args, configured)
    if args.calibration_policy is not None:
        settings = ERA5Settings.model_validate(
            settings.model_dump() | {"calibration_policy": args.calibration_policy}
        )
    configure_logging(settings)
    require_persistent_calibration(settings)
    resolve_calibration(settings, lambda: run(settings))


if __name__ == "__main__":
    main()
