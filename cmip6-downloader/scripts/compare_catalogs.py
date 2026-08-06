#!/usr/bin/env python3
"""Compare and resolve the AWS and Google CMIP6 catalogs."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from src.compare import compare_cmip6_catalogs
from src.config import Settings, settings_with_overrides


def build_parser() -> argparse.ArgumentParser:
    """Build the catalog comparison command-line parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog-aws-path", type=Path)
    parser.add_argument("--catalog-google-path", type=Path)
    parser.add_argument("--filtered-catalog-dir", type=Path)
    parser.add_argument("--preferred-catalog-path", type=Path)
    parser.add_argument("--source-ids", nargs="+")
    parser.add_argument("--experiment-ids", nargs="+")
    parser.add_argument("--table-ids", nargs="+")
    parser.add_argument("--variable-ids", nargs="+")
    parser.add_argument("--grid-labels", nargs="+")
    parser.add_argument("--member-ids", nargs="*")
    parser.add_argument("--provider-priority", nargs=2, choices=("aws", "google"))
    return parser


def settings_from_args(args: argparse.Namespace) -> Settings:
    """Load settings and apply only explicit CLI arguments."""
    return settings_with_overrides(Settings(), vars(args))


def main() -> None:
    """Run catalog comparison."""
    settings = settings_from_args(build_parser().parse_args())
    logging.basicConfig(level=settings.log_level.value, format=settings.log_format)
    compare_cmip6_catalogs(settings)


if __name__ == "__main__":
    main()
