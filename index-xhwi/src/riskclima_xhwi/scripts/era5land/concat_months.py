import argparse
from collections.abc import Sequence
from pathlib import Path

from riskclima_xhwi.config.settings import ConcatInputPolicy, ERA5LandSettings
from riskclima_xhwi.io.writers import concat_monthly_netcdfs
from riskclima_xhwi.scripts.common import (
    add_existing_policy_argument,
    add_runtime_arguments,
    configure_logging,
    explicit_overrides,
)


def build_parser() -> argparse.ArgumentParser:
    """Build the ERA5-Land concatenation parser."""
    parser = argparse.ArgumentParser(description="Concatenate configured ERA5-Land parts.")
    add_runtime_arguments(parser)
    add_existing_policy_argument(parser, "--final-existing-policy", "final_existing_policy")
    parser.add_argument("--concat-input-policy", choices=[item.value for item in ConcatInputPolicy])
    return parser


def run(settings: ERA5LandSettings, current_parts: Sequence[Path] | None = None) -> Path:
    """Concatenate ERA5-Land parts selected by policy."""
    if settings.concat_input_policy is ConcatInputPolicy.CURRENT_RUN and current_parts is None:
        raise ValueError("current_run is only available through run-all, which supplies new parts")
    paths = (
        settings.matching_parts()
        if settings.concat_input_policy is ConcatInputPolicy.ALL_MATCHING_PARTS
        else list(current_parts or ())
    )
    return concat_monthly_netcdfs(
        paths,
        settings.final_output(),
        settings=settings,
        policy=settings.final_existing_policy,
    )


def main(argv: Sequence[str] | None = None) -> None:
    """Run ERA5-Land monthly concatenation."""
    settings = ERA5LandSettings()
    args = build_parser().parse_args(argv)
    settings = ERA5LandSettings.model_validate(
        settings.model_dump()
        | explicit_overrides(
            args, ("device", "log_level", "final_existing_policy", "concat_input_policy")
        )
    )
    configure_logging(settings)
    run(settings)


if __name__ == "__main__":
    main()
