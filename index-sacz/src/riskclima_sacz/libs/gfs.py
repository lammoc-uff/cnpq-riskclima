"""GFS acquisition helpers and SACZ atmospheric field catalog."""

import logging
from concurrent import futures
from datetime import datetime, timedelta
from pathlib import Path

import requests
from pydantic import BaseModel

from riskclima_sacz.libs import models

LOGGER = logging.getLogger(__name__)
REQUEST_TIMEOUT_SECONDS = 120
TIMESTEP_LIMIT_HOURS = 384


class ItemGFS(BaseModel):
    """Describe one GFS forecast file to download."""

    url: str
    date: datetime
    step: int
    forecast_date: datetime
    output_path: Path


def rda_download(
    dates: datetime | list[datetime],
    timestep: int,
    output_path: Path,
    login: str,
    password: str,
    workers: int = 2,
    cycle: str = "00",
) -> None:
    """Download GFS forecast files from the UCAR Research Data Archive.

    Historical and operational forecasts are available from UCAR dataset
    ``ds084.1``.

    Parameters
    ----------
    dates
        Initial forecast dates for which files are requested.
    timestep
        Forecast interval in hours, up to 384 hours.
    output_path
        Directory where GRIB2 files are stored.
    login
        Email address registered with UCAR.
    password
        Password for the UCAR account.
    workers
        Maximum number of concurrent downloads.
    cycle
        Two-digit forecast cycle included in the remote and local filenames.
    """
    if timestep <= 0:
        raise ValueError("timestep must be greater than zero")
    if workers <= 0:
        raise ValueError("workers must be greater than zero")

    dspath = "https://rda.ucar.edu/data/ds084.1"
    downloads: list[ItemGFS] = []
    requested_dates = dates if isinstance(dates, list) else [dates]
    output_path.mkdir(parents=True, exist_ok=True)

    for date in requested_dates:
        date_fmt = date.strftime("%Y%m%d")

        for step in range(0, TIMESTEP_LIMIT_HOURS + 1, timestep):
            step_fmt = f"{step}".rjust(3, "0")
            forecast_date = date + timedelta(hours=step)

            url = f"{dspath}/{date.year}/{date_fmt}/gfs.0p25.{date_fmt}{cycle}.f{step_fmt}.grib2"

            downloads.append(
                ItemGFS(
                    url=url,
                    date=date,
                    step=step,
                    forecast_date=forecast_date,
                    output_path=output_path,
                )
            )

    def write(item: ItemGFS) -> None:
        login_url = "https://rda.ucar.edu/cgi-bin/login"
        date_fmt = item.date.strftime("%Y%m%d")
        step_fmt = f"{item.step}".rjust(3, "0")
        output_file_path = item.output_path / (f"gfs.0p25.{date_fmt}{cycle}.f{step_fmt}.grib2")

        if output_file_path.is_file():
            LOGGER.info("GFS file already exists", extra={"path": output_file_path})
            return

        values = {"email": login, "passwd": password, "action": "login"}
        ret = requests.post(login_url, data=values, timeout=REQUEST_TIMEOUT_SECONDS)
        ret.raise_for_status()
        res = requests.get(
            item.url,
            cookies=ret.cookies,
            allow_redirects=True,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        res.raise_for_status()

        with output_file_path.open("wb") as file:
            file.write(res.content)
        LOGGER.info("GFS file downloaded", extra={"path": output_file_path})

    with futures.ThreadPoolExecutor(max_workers=workers) as executor:
        downloads_in_progress = [executor.submit(write, item) for item in downloads]
        for download in downloads_in_progress:
            download.result()


feature_collection = [
    models.FeatureCollection(
        variable="w",
        nc_name="w",
        source="gfs",
        out_name="omega",
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
        variable="gh",
        nc_name="gh",
        source="gfs",
        out_name="hgt",
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
        variable="u",
        nc_name="u",
        source="gfs",
        out_name="uwnd",
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
        variable="v",
        nc_name="v",
        out_name="vwnd",
        source="gfs",
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
        source="gfs",
        out_name="div",
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
        source="gfs",
        out_name="vort",
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
        variable="u",
        nc_name="u",
        source="gfs",
        out_name="uwnd",
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
        variable="v",
        nc_name="v",
        source="gfs",
        out_name="vwnd",
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
        source="gfs",
        out_name="div",
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
