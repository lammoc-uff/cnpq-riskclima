import argparse
from collections.abc import Sequence

from riskclima_xhwi.config.settings import ERA5LandSettings
from riskclima_xhwi.io.writers import normalize_months
from riskclima_xhwi.scripts.common import (
    add_calibration_policy_argument,
    add_existing_policy_argument,
    add_month_arguments,
    configure_logging,
)
from riskclima_xhwi.scripts.era5land import concat_months, run_months
from riskclima_xhwi.scripts.era5land.common import add_common_arguments


def build_parser() -> argparse.ArgumentParser:
    """Build the complete ERA5-Land workflow parser."""
    parser = argparse.ArgumentParser(description="Run the configured ERA5-Land workflow.")
    add_common_arguments(parser)
    add_month_arguments(parser)
    add_calibration_policy_argument(parser)
    add_existing_policy_argument(parser, "--part-existing-policy", "part_existing_policy")
    add_existing_policy_argument(parser, "--final-existing-policy", "final_existing_policy")
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    """Run all ERA5-Land processing stages."""
    configured = ERA5LandSettings()
    args = build_parser().parse_args(argv)
    settings = run_months.settings_with_args(args, configured)
    if args.final_existing_policy is not None:
        settings = ERA5LandSettings.model_validate(
            settings.model_dump() | {"final_existing_policy": args.final_existing_policy}
        )
    configure_logging(settings)
    parts = run_months.run(settings, normalize_months(settings.months_to_run))
    concat_months.run(settings, parts)


if __name__ == "__main__":
    main()
