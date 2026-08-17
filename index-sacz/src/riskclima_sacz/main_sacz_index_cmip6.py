#!/usr/bin/env python3
"""
Calculate the South Atlantic Convergence Zone (SACZ) index from preprocessed
CMIP6 atmospheric fields.

The script applies the statistical model to the CMIP6 input time series,
computes the intermediate processing steps, and saves the daily SACZ index
for each index region.
"""

import sys
import warnings
from pathlib import Path
from typing import cast

import pandas as pd

from riskclima_sacz.config import SACZSettings, parse_settings
from riskclima_sacz.model import logistic_probability

warnings.simplefilter(action="ignore", category=FutureWarning)


# Configuration

SOURCE_ID = ""
EXPERIMENT_ID = ""
YEARS: list[int] = []

# Project root
SACZ_BASE = Path()
CMIP6_INPUT_DIR = Path()
CMIP6_INTERMEDIATES_DIR = Path()
CMIP6_OUTPUT_DIR = Path()
COEFFICIENTS_DIR = Path()


# Model constants

AREAS = ["AB", "C", "DE"]

VARIABLES = [
    "DIV200",
    "DIV850",
    "HGT500",
    "OMEGA500",
    "UWND200",
    "UWND850",
    "VWND200",
    "VWND850",
    "VORT200",
]

# Selected (PC index, beta row) pairs used in the regional linear combinations.

PCS_BETA = {
    "AB": [(1, 2), (2, 3), (3, 4), (6, 5), (7, 6)],
    "C": [(1, 2), (2, 3), (4, 4), (5, 5), (6, 6), (8, 7), (9, 8), (10, 9)],
    "DE": [(1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (8, 7), (9, 8), (11, 9)],
}

THRESHOLDS = {
    "AB": {"h1": 0.15, "h2": 0.30, "h3": 0.58},
    "C": {"h1": 0.14, "h2": 0.34, "h3": 0.52},
    "DE": {"h1": 0.12, "h2": 0.38, "h3": 0.52},
}


def configure(settings: SACZSettings) -> None:
    """Apply shared settings to the CMIP6 index workflow."""
    global CMIP6_INPUT_DIR, CMIP6_INTERMEDIATES_DIR, CMIP6_OUTPUT_DIR
    global COEFFICIENTS_DIR, EXPERIMENT_ID, SACZ_BASE, SOURCE_ID, YEARS
    SACZ_BASE = settings.path(Path())
    CMIP6_INPUT_DIR = settings.path(settings.cmip6_input_directory)
    CMIP6_INTERMEDIATES_DIR = settings.path(settings.cmip6_intermediates_directory)
    CMIP6_OUTPUT_DIR = settings.path(settings.cmip6_output_directory)
    COEFFICIENTS_DIR = settings.path(settings.coefficients_directory)
    SOURCE_ID = settings.cmip6_source_id
    EXPERIMENT_ID = settings.cmip6_experiment_id
    YEARS = list(range(settings.cmip6_start_year, settings.cmip6_end_year + 1))


# Statistical model


def process_year(year: int, source_id: str, experiment_id: str) -> None:
    inputdpath = CMIP6_INPUT_DIR / source_id / experiment_id / str(year)
    if not inputdpath.exists():
        print(f"[ERROR] Input directory not found: {inputdpath}")
        print("Run cmip6_process_sacz_index.py for this year before computing the index.")
        sys.exit(1)

    # Validate all required inputs before starting the calculation.
    missing_inputs = [v for v in VARIABLES if not (inputdpath / f"{v.lower()}.csv").exists()]
    if missing_inputs:
        print(f"[ERROR] Missing input files in {inputdpath}:")
        for v in missing_inputs:
            print(f"  - {v.lower()}.csv")
        print("Run cmip6_process_sacz_index.py for this year before computing the index.")
        sys.exit(1)

    cpath_step1 = COEFFICIENTS_DIR / "step1"
    cpath_step2 = COEFFICIENTS_DIR / "step2"
    cpath_step3 = COEFFICIENTS_DIR / "step3"

    interdpath = CMIP6_INTERMEDIATES_DIR / source_id / experiment_id / str(year)
    interdpath_step1 = interdpath / "step1"
    interdpath_step2 = interdpath / "step2"
    interdpath_step3 = interdpath / "step3"
    outputdpath = CMIP6_OUTPUT_DIR / source_id / experiment_id / str(year)

    for d in [interdpath_step1, interdpath_step2, interdpath_step3, outputdpath]:
        d.mkdir(exist_ok=True, parents=True)

    # Step 1: min-max scaling
    # Scale each subarea-variable series to [-1, 1] and subtract the reference mean.

    def scale(x: float, a: int = -1, b: int = 1) -> float:
        return (b - a) * ((x - fmin) / (fmax - fmin)) + a

    for variable in VARIABLES:
        dcomp_variable = []
        for area in AREAS:
            dpath = inputdpath / f"{variable.lower()}.csv"
            d = pd.read_csv(dpath, parse_dates=["time"]).set_index("time", drop=True)

            scale_coefs = (
                pd.read_csv(cpath_step1 / f"scale_coefs_{area}.csv")
                .set_index("var", drop=True)
                .filter(regex=variable, axis=0)
            )

            dcomp_area = []
            for idx, _coef in scale_coefs.iterrows():
                idx = str(idx)
                fmin = cast(float, scale_coefs.at[idx, "min"])
                fmax = cast(float, scale_coefs.at[idx, "max"])
                dsel = d.filter(regex=idx.split("_")[0], axis=1)
                dscl = dsel.apply(scale) - cast(float, scale_coefs.at[idx, "mean"])
                dcomp_area.append(dscl)

            dcomp_variable.append(pd.concat(dcomp_area, axis=1))

        pd.concat(dcomp_variable, axis=1).to_csv(interdpath_step1 / f"{variable}.csv")

    # Step 2: principal component scores
    # Apply the retained PCA weights to the normalized subarea-variable series.

    for area in AREAS:
        pc_weights = pd.read_csv(cpath_step2 / f"pc_weights_{area}.csv").set_index("PC")

        pc_comp = {}
        for npc in range(1, pc_weights.shape[0] + 1):
            weighted_total = []
            for variable in VARIABLES:
                dscl = (
                    pd.read_csv(
                        interdpath_step1 / f"{variable}.csv",
                        parse_dates=["time"],
                    )
                    .set_index("time")
                    .filter(regex=area, axis=1)
                )
                for subarea in dscl.columns:
                    weight = cast(float, pc_weights.at[npc, f"{subarea}_{variable}"])
                    weighted_total.append(dscl[[subarea]] * weight)

            pc_comp[npc] = pd.concat(weighted_total, axis=1).sum(axis=1)

        pd.DataFrame(pc_comp).to_csv(interdpath_step2 / f"{area}.csv")

    # Step 3: linear combination with logistic-regression coefficients
    # Combine the selected PCs using the regional beta coefficients and intercept.

    for area in AREAS:
        betas = pd.read_csv(cpath_step3 / f"betas_{area}.csv").set_index("PC")
        intercept = cast(float, betas.at[1, "beta"])

        pcweighted = pd.read_csv(interdpath_step2 / f"{area}.csv").set_index("time")

        comp = []
        for pc, beta_row in PCS_BETA[area]:
            pc_series = pcweighted[[str(pc)]].astype(float)
            comp.append(pc_series * cast(float, betas.at[beta_row, "beta"]))

        combined = pd.concat(comp, axis=1).sum(axis=1).to_frame()
        combined = combined.rename({0: area}, axis=1)
        combined = combined + intercept
        combined.to_csv(interdpath_step3 / f"{area}.csv")

    # Step 4: logistic classification
    # Map the linear score to [0, 1], with higher values indicating stronger
    # agreement with the atmospheric configuration associated with SACZ.

    for area in AREAS:
        betaweighted = pd.read_csv(interdpath_step3 / f"{area}.csv").set_index("time").astype(float)
        classified = betaweighted.map(logistic_probability)
        classified.index = pd.to_datetime(classified.index)
        classified.to_csv(outputdpath / f"{area}.csv")

    print(f"SACZ index for {source_id}/{experiment_id}/{year} completed.")
    print(f"  Output: {outputdpath}")


def main(arguments: list[str] | None = None) -> None:
    """Run the CMIP6 index calculation for every configured year."""
    settings = parse_settings(arguments)
    configure(settings)
    for year in YEARS:
        process_year(year, SOURCE_ID, EXPERIMENT_ID)


if __name__ == "__main__":
    main()
