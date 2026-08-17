"""
Calculate the South Atlantic Convergence Zone (SACZ) index from ERA5-derived
atmospheric input fields.

The script applies the statistical model to the preprocessed ERA5 time series,
computes the intermediate processing steps, and saves the daily SACZ index
for each index region.
"""

import warnings
from pathlib import Path
from typing import cast

import pandas as pd

from riskclima_sacz.config import SACZSettings, parse_settings
from riskclima_sacz.model import logistic_probability

# Suppress known warnings
warnings.simplefilter(action="ignore", category=FutureWarning)
warnings.filterwarnings(
    "ignore", message="ecCodes 2.31.0 or higher is recommended. You are running version 2.24.2"
)

# Project root
SACZ_BASE = Path()
COEFFICIENTS_DIR = Path()
ERA5_INPUT_DIR = Path()
ERA5_INTERMEDIATES_DIR = Path()
ERA5_OUTPUT_DIR = Path()

# Data source
data_source = "era5"

# Model coefficient paths
cpath_step1 = Path()
cpath_step2 = Path()
cpath_step3 = Path()

areas = ["AB", "C", "DE"]
variables = [
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


def configure(settings: SACZSettings) -> None:
    """Apply shared settings to the ERA5 index workflow."""
    global SACZ_BASE, COEFFICIENTS_DIR, ERA5_INPUT_DIR, ERA5_INTERMEDIATES_DIR
    global ERA5_OUTPUT_DIR, cpath_step1, cpath_step2, cpath_step3
    if settings.era5_year is None:
        raise ValueError("ERA5_YEAR must be set in .env or supplied with --era5-year")
    SACZ_BASE = settings.path(Path())
    COEFFICIENTS_DIR = settings.path(settings.coefficients_directory)
    ERA5_INPUT_DIR = settings.path(settings.era5_input_directory)
    ERA5_INTERMEDIATES_DIR = settings.path(settings.era5_intermediates_directory)
    ERA5_OUTPUT_DIR = settings.path(settings.era5_output_directory)
    cpath_step1 = COEFFICIENTS_DIR / "step1"
    cpath_step2 = COEFFICIENTS_DIR / "step2"
    cpath_step3 = COEFFICIENTS_DIR / "step3"


# Process one year
def process_year(year: int, data_source: str) -> None:
    date_str = str(year)

    # INPUT
    inputdpath = ERA5_INPUT_DIR / date_str
    if not inputdpath.exists():
        print(f"No data for year {year}. Skipping.")
        return

    # INTERMEDIARY
    interdpath = ERA5_INTERMEDIATES_DIR / date_str
    interdpath_step1 = interdpath / "step1"
    interdpath_step2 = interdpath / "step2"
    interdpath_step3 = interdpath / "step3"

    # OUTPUT
    outputdpath = ERA5_OUTPUT_DIR / date_str

    # Create output directories
    interdpath.mkdir(exist_ok=True, parents=True)
    interdpath_step1.mkdir(exist_ok=True, parents=True)
    interdpath_step2.mkdir(exist_ok=True, parents=True)
    interdpath_step3.mkdir(exist_ok=True, parents=True)
    outputdpath.mkdir(exist_ok=True, parents=True)

    # Step 1: min-max scaling
    def scale(x: float, a: int = -1, b: int = 1) -> float:
        return (b - a) * ((x - fmin) / (fmax - fmin)) + a

    for variable in variables:
        dcomp_variable = []
        for area in areas:
            dpath = inputdpath / f"{variable.lower()}.csv"
            d = pd.read_csv(dpath, parse_dates=["time"]).set_index("time", drop=True)

            dscalepath = cpath_step1 / f"scale_coefs_{area}.csv"
            scale_coefs = (
                pd.read_csv(dscalepath).set_index("var", drop=True).filter(regex=variable, axis=0)
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

        interoutpath = interdpath_step1 / f"{variable}.csv"
        pd.concat(dcomp_variable, axis=1).to_csv(interoutpath)

    # Step 2: principal component scores
    for area in areas:
        weights_path = cpath_step2 / f"pc_weights_{area}.csv"
        pc_weights = pd.read_csv(weights_path).set_index("PC")

        pc_comp = {}
        for npc in range(1, pc_weights.shape[0] + 1):
            weighted_total = []
            for variable in variables:
                dir_input = interdpath_step1 / f"{variable}.csv"
                dscl = pd.read_csv(dir_input, parse_dates=["time"]).set_index("time")
                dscl = dscl.filter(regex=area, axis=1)

                for subarea in dscl.columns:
                    weight = cast(float, pc_weights.at[npc, f"{subarea}_{variable}"])
                    weighted = dscl.loc[:, [subarea]] * weight
                    weighted_total.append(weighted)

            pc_comp.update({npc: pd.concat(weighted_total, axis=1).sum(axis=1)})

        pc_comp_outpath = interdpath_step2 / f"{area}.csv"
        pd.concat(pc_comp, axis=1).to_csv(pc_comp_outpath)

    # Step 3: linear combination
    pcs_beta = {
        "AB": [(1, 2), (2, 3), (3, 4), (6, 5), (7, 6)],
        "C": [(1, 2), (2, 3), (4, 4), (5, 5), (6, 6), (8, 7), (9, 8), (10, 9)],
        "DE": [(1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (8, 7), (9, 8), (11, 9)],
    }

    for area in areas:
        betas_path = cpath_step3 / f"betas_{area}.csv"
        betas = pd.read_csv(betas_path).set_index("PC")

        comp = []
        for pc, beta in pcs_beta[area]:
            pcweighted_path = interdpath_step2 / f"{area}.csv"
            pcweighted = pd.read_csv(pcweighted_path).set_index("time")
            pc_series = pcweighted.loc[:, [str(pc)]].astype(float)
            betaweighted = pc_series * cast(float, betas.at[beta, "beta"])
            comp.append(betaweighted)

        combined = pd.concat(comp, axis=1).sum(axis=1).to_frame()
        combined = combined.rename({0: f"{area}"}, axis=1)
        combined = combined + cast(float, betas.at[1, "beta"])

        interoutpath = interdpath_step3 / f"{area}.csv"
        combined.to_csv(interoutpath)

    # Step 4: logistic classification
    for area in areas:
        betaweighted_path = interdpath_step3 / f"{area}.csv"
        betaweighted = pd.read_csv(betaweighted_path).set_index("time").astype(float)

        classified = betaweighted.map(logistic_probability)
        classified.columns = [area]
        classified.index = pd.to_datetime(classified.index)

        classified_path = outputdpath / f"{area}.csv"
        classified.to_csv(classified_path)

    print(f"SACZ index calculation for year {year} completed successfully.")


def main(arguments: list[str] | None = None) -> None:
    """Calculate the ERA5 SACZ index for the configured year."""
    settings = parse_settings(arguments)
    configure(settings)
    if settings.era5_year is None:
        raise RuntimeError("ERA5 index was not configured")
    process_year(settings.era5_year, data_source)


if __name__ == "__main__":
    main()
