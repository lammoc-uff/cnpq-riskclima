"""NCEP Reanalysis 2 acquisition helpers and SACZ field catalog."""

import logging
from concurrent import futures
from pathlib import Path

import requests
from pydantic import BaseModel

from riskclima_sacz.libs import models

LOGGER = logging.getLogger(__name__)
REQUEST_TIMEOUT_SECONDS = 120
MAX_DOWNLOAD_WORKERS = 4


class ItemNCEP(BaseModel):
    """Describe one annual NCEP field to download."""

    url: str
    variable: str
    year: int
    output_path: Path


def download(
    iyear: int,
    fyear: int,
    variables: list[str],
    output_path: Path,
    level_type: str = "pressure",
) -> None:
    """Download annual pressure-level fields from NCEP Reanalysis 2.

    Parameters
    ----------
    iyear
        First year to download, inclusive.
    fyear
        Last year to download, inclusive.
    variables
        NOAA variable names to download.
    output_path
        Directory where NetCDF files are stored.
    level_type
        NOAA dataset category containing the requested variables.
    """
    output_path.mkdir(parents=True, exist_ok=True)
    downloads: list[ItemNCEP] = []
    for variable in variables:
        for year in range(iyear, fyear + 1):
            url = (
                f"https://downloads.psl.noaa.gov/Datasets/"
                f"ncep.reanalysis2.dailyavgs/{level_type}/{variable}.{year}.nc"
            )
            downloads.append(
                ItemNCEP(url=url, variable=variable, year=year, output_path=output_path)
            )

    def persist(item: ItemNCEP) -> None:
        output_file_path = item.output_path / f"{item.variable}{item.year}.nc"

        if output_file_path.is_file():
            LOGGER.info("NCEP file already exists", extra={"path": output_file_path})
            return

        response = requests.get(item.url, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()

        with output_file_path.open("wb") as file:
            file.write(response.content)

        LOGGER.info("NCEP file downloaded", extra={"path": output_file_path})

    with futures.ThreadPoolExecutor(max_workers=MAX_DOWNLOAD_WORKERS) as executor:
        downloads_in_progress = [executor.submit(persist, item) for item in downloads]
        for download_in_progress in downloads_in_progress:
            download_in_progress.result()


feature_collection = [
    models.FeatureCollection(
        variable="omega",
        nc_name="omega",
        out_name="omega",
        source="ncep reanalysis 2",
        level=500,
        features=[
            models.Feature(area="AB1", shape="AB_OMEGA500_1"),
            models.Feature(area="AB2", shape="AB_OMEGA500_2"),
            models.Feature(area="C1", shape="C_OMEGA500_1"),
            models.Feature(area="C2", shape="C_OMEGA500_2"),
            models.Feature(area="C3", shape="C_OMEGA500_3"),
            models.Feature(area="DE1", shape="DE_OMEGA500_1"),
            models.Feature(area="DE2", shape="DE_OMEGA500_2"),
        ],
    ),
    models.FeatureCollection(
        variable="hgt",
        nc_name="hgt",
        out_name="hgt",
        source="ncep reanalysis 2",
        level=500,
        features=[
            models.Feature(area="AB1", shape="AB_HGT500_1"),
            models.Feature(area="AB2", shape="AB_HGT500_2"),
            models.Feature(area="C1", shape="C_HGT500_1"),
            models.Feature(area="C2", shape="C_HGT500_2"),
            models.Feature(area="DE2", shape="DE_HGT500_2"),
            models.Feature(area="DE3", shape="DE_HGT500_3"),
        ],
    ),
    models.FeatureCollection(
        variable="uwnd",
        nc_name="uwnd",
        out_name="uwnd",
        source="ncep reanalysis 2",
        level=200,
        features=[
            models.Feature(area="AB1", shape="AB_U200_1"),
            models.Feature(area="AB2", shape="AB_U200_2"),
            models.Feature(area="AB3", shape="AB_U200_3"),
            models.Feature(area="C1", shape="C_U200_1"),
            models.Feature(area="C2", shape="C_U200_2"),
            models.Feature(area="C3", shape="C_U200_3"),
            models.Feature(area="DE1", shape="DE_U200_1"),
            models.Feature(area="DE2", shape="DE_U200_2"),
            models.Feature(area="DE3", shape="DE_U200_3"),
            models.Feature(area="DE4", shape="DE_U200_4"),
            models.Feature(area="DE5", shape="DE_U200_5"),
        ],
    ),
    models.FeatureCollection(
        variable="vwnd",
        nc_name="vwnd",
        out_name="vwnd",
        source="ncep reanalysis 2",
        level=200,
        features=[
            models.Feature(area="AB1", shape="AB_V200_1"),
            models.Feature(area="AB2", shape="AB_V200_2"),
            models.Feature(area="AB3", shape="AB_V200_3"),
            models.Feature(area="C1", shape="C_V200_1"),
            models.Feature(area="C2", shape="C_V200_2"),
            models.Feature(area="C3", shape="C_V200_3"),
            models.Feature(area="DE1", shape="DE_V200_1"),
            models.Feature(area="DE2", shape="DE_V200_2"),
            models.Feature(area="DE3", shape="DE_V200_3"),
            models.Feature(area="DE4", shape="DE_V200_4"),
        ],
    ),
    models.FeatureCollection(
        variable="div",
        nc_name="divergence",
        out_name="divergence",
        source="ncep reanalysis 2",
        level=200,
        features=[
            models.Feature(area="AB1", shape="AB_DIV200_1"),
            models.Feature(area="AB2", shape="AB_DIV200_2"),
            models.Feature(area="C1", shape="C_DIV200_1"),
            models.Feature(area="C2", shape="C_DIV200_2"),
            models.Feature(area="DE1", shape="DE_DIV200_1"),
            models.Feature(area="DE2", shape="DE_DIV200_2"),
        ],
    ),
    models.FeatureCollection(
        variable="vort",
        nc_name="vorticity",
        out_name="vorticity",
        source="ncep reanalysis 2",
        level=200,
        features=[
            models.Feature(area="AB1", shape="AB_VORT200_1"),
            models.Feature(area="AB2", shape="AB_VORT200_2"),
            models.Feature(area="AB3", shape="AB_VORT200_3"),
            models.Feature(area="C1", shape="C_VORT200_1"),
            models.Feature(area="C2", shape="C_VORT200_2"),
            models.Feature(area="C3", shape="C_VORT200_3"),
            models.Feature(area="DE1", shape="DE_VORT200_1"),
            models.Feature(area="DE2", shape="DE_VORT200_2"),
            models.Feature(area="DE3", shape="DE_VORT200_3"),
        ],
    ),
    models.FeatureCollection(
        variable="uwnd",
        nc_name="uwnd",
        out_name="uwnd",
        source="ncep reanalysis 2",
        level=850,
        features=[
            models.Feature(area="AB1", shape="AB_U850_1"),
            models.Feature(area="AB2", shape="AB_U850_2"),
            models.Feature(area="C1", shape="C_U850_1"),
            models.Feature(area="C2", shape="C_U850_2"),
            models.Feature(area="DE1", shape="DE_U850_1"),
        ],
    ),
    models.FeatureCollection(
        variable="vwnd",
        nc_name="vwnd",
        out_name="vwnd",
        source="ncep reanalysis 2",
        level=850,
        features=[
            models.Feature(area="AB1", shape="AB_V850_1"),
            models.Feature(area="AB2", shape="AB_V850_2"),
            models.Feature(area="AB3", shape="AB_V850_3"),
            models.Feature(area="C1", shape="C_V850_1"),
            models.Feature(area="C2", shape="C_V850_2"),
            models.Feature(area="C3", shape="C_V850_3"),
            models.Feature(area="DE1", shape="DE_V850_1"),
            models.Feature(area="DE2", shape="DE_V850_2"),
            models.Feature(area="DE3", shape="DE_V850_3"),
        ],
    ),
    models.FeatureCollection(
        variable="div",
        nc_name="divergence",
        out_name="divergence",
        source="ncep reanalysis 2",
        level=850,
        features=[
            models.Feature(area="AB1", shape="AB_DIV850_1"),
            models.Feature(area="AB2", shape="AB_DIV850_2"),
            models.Feature(area="C1", shape="C_DIV850_1"),
            models.Feature(area="C2", shape="C_DIV850_2"),
            models.Feature(area="DE1", shape="DE_DIV850_1"),
            models.Feature(area="DE2", shape="DE_DIV850_2"),
        ],
    ),
]
