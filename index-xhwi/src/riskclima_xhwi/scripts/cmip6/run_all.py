import argparse
from collections.abc import Sequence

from riskclima_xhwi.config.settings import CMIP6Settings
from riskclima_xhwi.io.writers import normalize_months
from riskclima_xhwi.scripts.cmip6 import concat_months, run_months
from riskclima_xhwi.scripts.cmip6.common import add_common_arguments, add_scenario_argument
from riskclima_xhwi.scripts.common import (
    add_calibration_policy_argument,
    add_existing_policy_argument,
    add_month_arguments,
    configure_logging,
)


def build_parser(settings: CMIP6Settings | None = None) -> argparse.ArgumentParser:
    """Build the complete CMIP6 workflow parser."""
    parser = argparse.ArgumentParser(description="Run the complete configured CMIP6 workflow.")
    add_common_arguments(parser)
    add_scenario_argument(parser, (settings or CMIP6Settings()).scenarios, plural=True)
    add_month_arguments(parser)
    add_calibration_policy_argument(parser)
    add_existing_policy_argument(parser, "--part-existing-policy", "part_existing_policy")
    add_existing_policy_argument(parser, "--final-existing-policy", "final_existing_policy")
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    """Run calibration, parts, and final output for configured scenarios."""
    configured = CMIP6Settings()
    args = build_parser(configured).parse_args(argv)
    settings = run_months.settings_with_args(args, configured)
    updates: dict[str, str | list[str]] = {}
    if args.scenarios is not None:
        updates["scenarios"] = args.scenarios
        updates["default_scenario"] = args.scenarios[0]
    if args.final_existing_policy is not None:
        updates["final_existing_policy"] = args.final_existing_policy
    settings = CMIP6Settings.model_validate(settings.model_dump() | updates)
    configure_logging(settings)
    months = normalize_months(settings.months_to_run)
    for scenario in settings.scenarios:
        parts = run_months.run(settings, scenario, months)
        concat_months.run(settings, scenario, parts)


if __name__ == "__main__":
    main()
