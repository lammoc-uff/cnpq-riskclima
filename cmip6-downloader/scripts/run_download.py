#!/usr/bin/env python3
"""Download and preprocess assets from the preferred CMIP6 catalog."""

from __future__ import annotations

import argparse
import logging
from datetime import date
from pathlib import Path

from src.config import (
    Alignment,
    CurvilinearPolicy,
    EnsembleMode,
    ExistingPolicy,
    Settings,
    settings_with_overrides,
)
from src.downloader import CMIP6Downloader


def iso_date(value: str) -> date:
    """Parse an ISO date for argparse."""
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(str(error)) from error


def build_parser() -> argparse.ArgumentParser:
    """Build the download command-line parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--preferred-catalog-path", type=Path)
    parser.add_argument("--downloads-dir", type=Path)
    parser.add_argument("--source-ids", nargs="+")
    parser.add_argument("--experiment-ids", nargs="+")
    parser.add_argument("--table-ids", nargs="+")
    parser.add_argument("--variable-ids", nargs="+")
    parser.add_argument("--grid-labels", nargs="+")
    parser.add_argument("--member-ids", nargs="*")
    parser.add_argument("--historical-start", type=iso_date)
    parser.add_argument("--historical-end", type=iso_date)
    parser.add_argument("--future-start", type=iso_date)
    parser.add_argument("--future-end", type=iso_date)
    parser.add_argument("--latitude-min", type=float)
    parser.add_argument("--latitude-max", type=float)
    parser.add_argument("--longitude-min", type=float)
    parser.add_argument("--longitude-max", type=float)
    parser.add_argument("--spatial-subset", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--curvilinear-policy", choices=tuple(CurvilinearPolicy))
    parser.add_argument("--max-workers", type=int)
    parser.add_argument("--time-chunk-size", type=int)
    parser.add_argument("--existing-policy", choices=tuple(ExistingPolicy))
    parser.add_argument("--ensemble-mode", choices=tuple(EnsembleMode))
    parser.add_argument("--ensemble-alignment", choices=tuple(Alignment))
    parser.add_argument("--cleanup-members", action=argparse.BooleanOptionalAction, default=None)
    return parser


def settings_from_args(args: argparse.Namespace) -> Settings:
    """Load settings and apply only explicit CLI arguments."""
    return settings_with_overrides(Settings(), vars(args))


def main() -> None:
    """Run the grouped download pipeline."""
    settings = settings_from_args(build_parser().parse_args())
    logging.basicConfig(level=settings.log_level.value, format=settings.log_format)
    CMIP6Downloader(settings).run()


if __name__ == "__main__":
    main()
