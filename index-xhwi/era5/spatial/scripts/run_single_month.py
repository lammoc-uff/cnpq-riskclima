#!/usr/bin/env python3
"""
Run the XHWI (Extreme Heatwave Index) pipeline for a single month.

This script allows selective execution of the full pipeline for a chosen month,
useful for debugging or reprocessing specific months without running the entire year.
"""

import sys
from src.pipeline.main_pipeline import run_all_months

def main():
    if len(sys.argv) != 2:
        print("Usage: python run_single_month.py <month_number>")
        print("Example: python run_single_month.py 7  # July")
        sys.exit(1)

    try:
        month = int(sys.argv[1])
        if not (1 <= month <= 12):
            raise ValueError
    except ValueError:
        print("❌ Invalid month. Please enter an integer between 1 and 12.")
        sys.exit(1)

    # Run the pipeline for the selected month only
    run_all_months(months=[month])


if __name__ == "__main__":
    main()
