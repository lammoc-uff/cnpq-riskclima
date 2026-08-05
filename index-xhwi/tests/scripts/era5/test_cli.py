import argparse
import importlib
from collections.abc import Callable

import pytest

from riskclima_xhwi.config.settings import Device, XHWISettings
from riskclima_xhwi.scripts.cmip6.common import settings_from_args as cmip6_settings_from_args
from riskclima_xhwi.scripts.cmip6.run_months import build_parser as build_cmip6_months_parser
from riskclima_xhwi.scripts.era5.common import settings_from_args
from riskclima_xhwi.scripts.era5.run_months import build_parser as build_era5_months_parser
from riskclima_xhwi.scripts.era5land.common import settings_from_args as land_settings_from_args
from riskclima_xhwi.scripts.era5land.run_months import build_parser as build_land_months_parser

CLI_MODULES = (
    "riskclima_xhwi.scripts.era5.make_calibration",
    "riskclima_xhwi.scripts.era5.run_months",
    "riskclima_xhwi.scripts.era5.concat_months",
    "riskclima_xhwi.scripts.era5.run_all",
    "riskclima_xhwi.scripts.era5land.make_calibration",
    "riskclima_xhwi.scripts.era5land.run_months",
    "riskclima_xhwi.scripts.era5land.concat_months",
    "riskclima_xhwi.scripts.era5land.run_all",
    "riskclima_xhwi.scripts.cmip6.make_calibration",
    "riskclima_xhwi.scripts.cmip6.run_months",
    "riskclima_xhwi.scripts.cmip6.concat_months",
    "riskclima_xhwi.scripts.cmip6.run_all",
)


def test_package_imports() -> None:
    package = importlib.import_module("riskclima_xhwi")

    assert package.__version__ == "0.1.0"


@pytest.mark.parametrize("module_name", CLI_MODULES)
def test_cli_module_imports_and_exposes_help(module_name: str) -> None:
    module = importlib.import_module(module_name)

    help_text = module.build_parser().format_help()

    assert "usage:" in help_text
    assert module.main is not None


def test_explicit_cli_override_wins_over_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ERA5_DEVICE", "cpu")
    args = build_era5_months_parser().parse_args(["--device", "auto"])

    settings = settings_from_args(args)

    assert settings.device is Device.AUTO


@pytest.mark.parametrize(
    ("environment_name", "build_parser", "settings_loader"),
    [
        ("ERA5_DEVICE", build_era5_months_parser, settings_from_args),
        ("ERA5LAND_DEVICE", build_land_months_parser, land_settings_from_args),
        ("CMIP6_DEVICE", build_cmip6_months_parser, cmip6_settings_from_args),
    ],
)
def test_cli_precedence_for_each_source_family(
    monkeypatch: pytest.MonkeyPatch,
    environment_name: str,
    build_parser: Callable[[], argparse.ArgumentParser],
    settings_loader: Callable[[argparse.Namespace], XHWISettings],
) -> None:
    monkeypatch.setenv(environment_name, "cpu")
    parser = build_parser()
    args = parser.parse_args(["--device", "auto"])

    settings = settings_loader(args)

    assert settings.device is Device.AUTO
