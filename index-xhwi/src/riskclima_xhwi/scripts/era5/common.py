import argparse

from riskclima_xhwi.config.settings import ERA5Settings
from riskclima_xhwi.scripts.common import add_runtime_arguments, explicit_overrides


def add_common_arguments(parser: argparse.ArgumentParser) -> None:
    """Add ERA5 temporary setting overrides."""
    add_runtime_arguments(parser)
    parser.add_argument("--zarr-url")


def settings_from_args(
    args: argparse.Namespace, settings: ERA5Settings | None = None
) -> ERA5Settings:
    """Load ERA5 settings and apply explicitly supplied CLI overrides."""
    settings = settings or ERA5Settings()
    return ERA5Settings.model_validate(
        settings.model_dump() | explicit_overrides(args, ("device", "log_level", "zarr_url"))
    )
