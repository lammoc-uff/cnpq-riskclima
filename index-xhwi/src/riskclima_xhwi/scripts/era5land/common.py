import argparse

from riskclima_xhwi.config.settings import ERA5LandSettings
from riskclima_xhwi.scripts.common import add_runtime_arguments, explicit_overrides


def add_common_arguments(parser: argparse.ArgumentParser) -> None:
    """Add ERA5-Land temporary setting overrides."""
    add_runtime_arguments(parser)
    parser.add_argument("--zarr-url")


def settings_from_args(
    args: argparse.Namespace, settings: ERA5LandSettings | None = None
) -> ERA5LandSettings:
    """Load ERA5-Land settings and apply explicitly supplied CLI overrides."""
    settings = settings or ERA5LandSettings()
    return ERA5LandSettings.model_validate(
        settings.model_dump() | explicit_overrides(args, ("device", "log_level", "zarr_url"))
    )
