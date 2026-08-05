import argparse
import logging
from collections.abc import Sequence
from pathlib import Path

from riskclima_xhwi.config.settings import CMIP6Settings, ExistingFilePolicy
from riskclima_xhwi.io.writers import write_calibration_netcdf
from riskclima_xhwi.pipeline.data_access import open_cmip6_calibration
from riskclima_xhwi.pipeline.policies import require_persistent_calibration, resolve_calibration
from riskclima_xhwi.scripts.cmip6.common import add_common_arguments, settings_from_args
from riskclima_xhwi.scripts.common import add_calibration_policy_argument, configure_logging

LOGGER = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    """Build the CMIP6 calibration parser."""
    parser = argparse.ArgumentParser(description="Stage the configured CMIP6 calibration.")
    add_common_arguments(parser)
    add_calibration_policy_argument(parser)
    return parser


def run(settings: CMIP6Settings) -> Path:
    """Build and replace the configured CMIP6 calibration file."""
    calibration = open_cmip6_calibration(settings)
    calibration.name = "tasmax_calibration"
    calibration.attrs.update(
        {
            "long_name": "Native daily maximum temperature for XHWI calibration",
            "units": "degC",
            "source_variable": settings.variable_tasmax,
            "model_id": settings.model,
            "experiment_id": "historical",
            "grid_label": settings.grid,
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
    """Run CMIP6 calibration according to policy."""
    configured = CMIP6Settings()
    args = build_parser().parse_args(argv)
    settings = settings_from_args(args, configured)
    if args.calibration_policy is not None:
        settings = CMIP6Settings.model_validate(
            settings.model_dump() | {"calibration_policy": args.calibration_policy}
        )
    configure_logging(settings)
    require_persistent_calibration(settings)
    resolve_calibration(settings, lambda: run(settings))


if __name__ == "__main__":
    main()
