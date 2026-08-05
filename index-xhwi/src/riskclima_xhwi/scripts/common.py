import argparse
import logging

from riskclima_xhwi.config.settings import (
    CalibrationPolicy,
    Device,
    ExistingFilePolicy,
    LogLevel,
    XHWISettings,
)


def configure_logging(settings: XHWISettings) -> None:
    """Configure textual command-line logging from settings."""
    logging.basicConfig(
        level=settings.log_level.value,
        format=settings.log_format,
        force=True,
    )


def add_runtime_arguments(parser: argparse.ArgumentParser) -> None:
    """Add temporary runtime overrides shared by processing commands."""
    parser.add_argument("--device", choices=[item.value for item in Device])
    parser.add_argument("--log-level", choices=[item.value for item in LogLevel])


def add_month_arguments(parser: argparse.ArgumentParser) -> None:
    """Add temporary calendar-month overrides."""
    parser.add_argument("--months-to-run", nargs="+", type=int, metavar="MONTH")


def add_calibration_policy_argument(parser: argparse.ArgumentParser) -> None:
    """Add a temporary calibration-policy override."""
    parser.add_argument("--calibration-policy", choices=[item.value for item in CalibrationPolicy])


def add_existing_policy_argument(
    parser: argparse.ArgumentParser, option: str, destination: str
) -> None:
    """Add a temporary existing-file policy override."""
    parser.add_argument(
        option,
        dest=destination,
        choices=[item.value for item in ExistingFilePolicy],
    )


def explicit_overrides(
    args: argparse.Namespace, fields: tuple[str, ...]
) -> dict[str, str | list[int]]:
    """Extract only explicitly supplied command-line setting overrides."""
    overrides: dict[str, str | list[int]] = {}
    for field in fields:
        value = getattr(args, field, None)
        if value is not None:
            overrides[field] = value
    return overrides
