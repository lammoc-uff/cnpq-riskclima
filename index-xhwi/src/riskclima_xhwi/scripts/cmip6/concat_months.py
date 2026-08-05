import argparse
from collections.abc import Sequence
from pathlib import Path

from riskclima_xhwi.config.settings import CMIP6Settings, ConcatInputPolicy
from riskclima_xhwi.io.writers import concat_monthly_netcdfs
from riskclima_xhwi.scripts.cmip6.common import (
    add_common_arguments,
    add_scenario_argument,
    settings_from_args,
)
from riskclima_xhwi.scripts.common import (
    add_existing_policy_argument,
    configure_logging,
)


def build_parser(settings: CMIP6Settings | None = None) -> argparse.ArgumentParser:
    """Build the CMIP6 concatenation parser."""
    parser = argparse.ArgumentParser(description="Concatenate configured CMIP6 parts.")
    add_common_arguments(parser)
    add_scenario_argument(parser, (settings or CMIP6Settings()).scenarios)
    add_existing_policy_argument(parser, "--final-existing-policy", "final_existing_policy")
    parser.add_argument("--concat-input-policy", choices=[item.value for item in ConcatInputPolicy])
    return parser


def settings_with_args(
    args: argparse.Namespace, settings: CMIP6Settings | None = None
) -> CMIP6Settings:
    """Load settings and apply concatenation overrides."""
    settings = settings_from_args(args, settings)
    updates: dict[str, str] = {}
    for field in ("default_scenario", "final_existing_policy", "concat_input_policy"):
        value = getattr(args, field)
        if value is not None:
            updates[field] = value
    return CMIP6Settings.model_validate(settings.model_dump() | updates)


def run(
    settings: CMIP6Settings,
    scenario: str,
    current_parts: Sequence[Path] | None = None,
) -> Path:
    """Concatenate CMIP6 parts selected by policy."""
    if settings.concat_input_policy is ConcatInputPolicy.CURRENT_RUN and current_parts is None:
        raise ValueError("current_run is only available through run-all, which supplies new parts")
    paths = (
        settings.matching_parts(scenario=scenario)
        if settings.concat_input_policy is ConcatInputPolicy.ALL_MATCHING_PARTS
        else list(current_parts or ())
    )
    return concat_monthly_netcdfs(
        paths,
        settings.final_output(scenario=scenario),
        settings=settings,
        scenario=scenario,
        policy=settings.final_existing_policy,
    )


def main(argv: Sequence[str] | None = None) -> None:
    """Run CMIP6 monthly concatenation."""
    configured = CMIP6Settings()
    args = build_parser(configured).parse_args(argv)
    settings = settings_with_args(args, configured)
    configure_logging(settings)
    run(settings, settings.default_scenario)


if __name__ == "__main__":
    main()
