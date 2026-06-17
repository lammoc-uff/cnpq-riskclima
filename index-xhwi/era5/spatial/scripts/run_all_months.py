#!/usr/bin/env python3
"""
Run the complete XHWI (Extreme Heatwave Index) pipeline for all months.

This script is the main entry point for batch execution. It loads calibration
and validation datasets, processes each month sequentially, and writes
hourly, daily, and monthly Zarr outputs to disk.
"""

from src.pipeline.main_pipeline import run_all_months

if __name__ == "__main__":
    run_all_months()
