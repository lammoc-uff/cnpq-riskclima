import argparse
import logging
from collections.abc import Sequence
from pathlib import Path

from riskclima_xhwi.config.settings import ERA5Settings, ExistingFilePolicy
from riskclima_xhwi.io.writers import (
    build_monthly_output_dataset,
    normalize_months,
    write_monthly_netcdf,
)
from riskclima_xhwi.pipeline.monthly_pipeline import compute_era5_monthly_xhwi_torch
from riskclima_xhwi.scripts.common import (
    add_calibration_policy_argument,
    add_existing_policy_argument,
    add_month_arguments,
    configure_logging,
)
from riskclima_xhwi.scripts.era5.common import add_common_arguments, settings_from_args

LOGGER = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    """Build the ERA5 monthly parser."""
    parser = argparse.ArgumentParser(description="Compute configured ERA5 calendar months.")
    add_common_arguments(parser)
    add_month_arguments(parser)
    add_calibration_policy_argument(parser)
    add_existing_policy_argument(parser, "--part-existing-policy", "part_existing_policy")
    return parser


def run(
    settings: ERA5Settings,
    months: Sequence[int],
) -> list[Path]:
    """Compute and write selected ERA5 calendar months."""
    selected_months = normalize_months(months)
    outputs = [settings.part_output(month) for month in selected_months]
    if settings.part_existing_policy is ExistingFilePolicy.FAIL:
        existing = next((output for output in outputs if output.exists()), None)
        if existing is not None:
            raise FileExistsError(f"File already exists: {existing}")
    pending = [output for output in outputs if not output.exists()]
    if settings.part_existing_policy is ExistingFilePolicy.SKIP and not pending:
        return outputs
    dataset = compute_era5_monthly_xhwi_torch(
        settings.zarr_url,
        months=selected_months,
        settings=settings,
    )
    written: list[Path] = []
    for month, output in zip(selected_months, outputs, strict=True):
        if output.exists() and settings.part_existing_policy is ExistingFilePolicy.SKIP:
            written.append(output)
            continue
        monthly = dataset["xhwi_monthly_accumulated"].sel(time=dataset["time.month"] == month)
        if monthly.sizes.get("time", 0) == 0:
            LOGGER.warning("No ERA5 output generated for month %02d", month)
            continue
        part = build_monthly_output_dataset(monthly, settings)
        written.append(
            write_monthly_netcdf(
                part, output, settings=settings, policy=settings.part_existing_policy
            )
        )
    return written


def settings_with_args(
    args: argparse.Namespace, settings: ERA5Settings | None = None
) -> ERA5Settings:
    """Load settings and apply monthly command overrides."""
    settings = settings_from_args(args, settings)
    updates: dict[str, str | list[int]] = {}
    for field in ("months_to_run", "calibration_policy", "part_existing_policy"):
        value = getattr(args, field)
        if value is not None:
            updates[field] = value
    return ERA5Settings.model_validate(settings.model_dump() | updates)


def main(argv: Sequence[str] | None = None) -> None:
    """Run ERA5 monthly processing."""
    configured = ERA5Settings()
    args = build_parser().parse_args(argv)
    settings = settings_with_args(args, configured)
    configure_logging(settings)
    months = normalize_months(settings.months_to_run)
    run(settings, months)


if __name__ == "__main__":
    main()
