import argparse
import logging
from collections.abc import Sequence
from pathlib import Path

from riskclima_xhwi.config.settings import CMIP6Settings, ExistingFilePolicy
from riskclima_xhwi.io.writers import (
    build_monthly_output_dataset,
    normalize_months,
    write_monthly_netcdf,
)
from riskclima_xhwi.pipeline.monthly_pipeline import compute_cmip6_monthly_xhwi_torch
from riskclima_xhwi.scripts.cmip6.common import (
    add_common_arguments,
    add_scenario_argument,
    settings_from_args,
)
from riskclima_xhwi.scripts.common import (
    add_calibration_policy_argument,
    add_existing_policy_argument,
    add_month_arguments,
    configure_logging,
)

LOGGER = logging.getLogger(__name__)


def build_parser(settings: CMIP6Settings | None = None) -> argparse.ArgumentParser:
    """Build the CMIP6 monthly parser."""
    parser = argparse.ArgumentParser(description="Compute configured CMIP6 calendar months.")
    add_common_arguments(parser)
    add_scenario_argument(parser, (settings or CMIP6Settings()).scenarios)
    add_month_arguments(parser)
    add_calibration_policy_argument(parser)
    add_existing_policy_argument(parser, "--part-existing-policy", "part_existing_policy")
    return parser


def settings_with_args(
    args: argparse.Namespace, settings: CMIP6Settings | None = None
) -> CMIP6Settings:
    """Load settings and apply monthly command overrides."""
    settings = settings_from_args(args, settings)
    updates: dict[str, str | list[int]] = {}
    for field in (
        "default_scenario",
        "months_to_run",
        "calibration_policy",
        "part_existing_policy",
    ):
        value = getattr(args, field, None)
        if value is not None:
            updates[field] = value
    return CMIP6Settings.model_validate(settings.model_dump() | updates)


def run(
    settings: CMIP6Settings,
    scenario: str,
    months: Sequence[int],
) -> list[Path]:
    """Compute and write selected months for one CMIP6 scenario."""
    if scenario not in settings.scenarios:
        raise ValueError(f"Scenario is not configured: {scenario}")
    selected_months = normalize_months(months)
    outputs = [settings.part_output(month, scenario=scenario) for month in selected_months]
    if settings.part_existing_policy is ExistingFilePolicy.FAIL:
        existing = next((output for output in outputs if output.exists()), None)
        if existing is not None:
            raise FileExistsError(f"File already exists: {existing}")
    pending = [output for output in outputs if not output.exists()]
    if settings.part_existing_policy is ExistingFilePolicy.SKIP and not pending:
        return outputs
    dataset = compute_cmip6_monthly_xhwi_torch(
        scenario,
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
            LOGGER.warning("No CMIP6 output generated for month %02d", month)
            continue
        part = build_monthly_output_dataset(monthly, settings, scenario=scenario)
        written.append(
            write_monthly_netcdf(
                part, output, settings=settings, policy=settings.part_existing_policy
            )
        )
    return written


def main(argv: Sequence[str] | None = None) -> None:
    """Run CMIP6 monthly processing."""
    configured = CMIP6Settings()
    args = build_parser(configured).parse_args(argv)
    settings = settings_with_args(args, configured)
    configure_logging(settings)
    run(
        settings,
        settings.default_scenario,
        normalize_months(settings.months_to_run),
    )


if __name__ == "__main__":
    main()
