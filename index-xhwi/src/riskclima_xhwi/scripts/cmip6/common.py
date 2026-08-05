import argparse

from riskclima_xhwi.config.settings import CMIP6Settings
from riskclima_xhwi.scripts.common import add_runtime_arguments, explicit_overrides


def add_common_arguments(parser: argparse.ArgumentParser) -> None:
    """Add CMIP6 temporary identity and runtime overrides."""
    add_runtime_arguments(parser)
    parser.add_argument("--model")
    parser.add_argument("--member")
    parser.add_argument("--grid")


def settings_from_args(
    args: argparse.Namespace, settings: CMIP6Settings | None = None
) -> CMIP6Settings:
    """Load CMIP6 settings and apply explicitly supplied CLI overrides."""
    settings = settings or CMIP6Settings()
    return CMIP6Settings.model_validate(
        settings.model_dump()
        | explicit_overrides(args, ("device", "log_level", "model", "member", "grid"))
    )


def add_scenario_argument(
    parser: argparse.ArgumentParser, scenarios: list[str], *, plural: bool = False
) -> None:
    """Add a scenario override whose valid values come from loaded settings."""
    option = "--scenarios" if plural else "--default-scenario"
    parser.add_argument(option, nargs="+" if plural else None, choices=scenarios)
