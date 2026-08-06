from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

import xarray as xr
from xclim.indices import standardized_precipitation_index
from xclim.indices.stats import standardized_index_fit_params

from riskclima_spi.config import SPISettings


@dataclass(frozen=True)
class SPISourceMetadata:
    """Source-specific descriptions recorded in an SPI product."""

    title: str
    source: str
    keywords: str
    input_variables: str
    input_frequency: str
    precipitation_conversion: str


def open_precipitation_dataset(path: Path) -> xr.Dataset:
    """Open a local NetCDF or Zarr precipitation dataset.

    Parameters
    ----------
    path
        Local NetCDF file or Zarr store.

    Returns
    -------
    xarray.Dataset
        Lazily opened precipitation dataset.
    """
    if path.suffix == ".zarr" or path.is_dir():
        return xr.open_zarr(path)
    return xr.open_dataset(path)


def calculate_spi(
    monthly_precipitation: xr.DataArray,
    calibration_precipitation: xr.DataArray,
    settings: SPISettings,
) -> xr.DataArray:
    """Calculate SPI from monthly accumulated precipitation.

    Parameters
    ----------
    monthly_precipitation
        Monthly accumulated precipitation to transform into SPI.
    calibration_precipitation
        Monthly accumulated precipitation used to fit the distribution.
    settings
        SPI fitting and period configuration.

    Returns
    -------
    xarray.DataArray
        Unitless SPI values for the configured application period.
    """
    calibration = calibration_precipitation.sel(
        time=slice(
            settings.spi_calibration_start.isoformat(),
            settings.spi_calibration_end.isoformat(),
        )
    )
    _validate_monthly_period(
        calibration,
        settings.spi_calibration_start,
        settings.spi_calibration_end,
        "calibration",
    )
    parameters = standardized_index_fit_params(
        calibration,
        freq=None,
        window=settings.spi_scale_months,
        dist=settings.spi_distribution,
        method=settings.spi_method,
        zero_inflated=True,
        fitkwargs={"floc": settings.spi_floc},
    )
    result = standardized_precipitation_index(pr=monthly_precipitation, params=parameters)
    result = result.sel(
        time=slice(
            settings.spi_application_start.isoformat(),
            settings.spi_application_end.isoformat(),
        )
    ).rename("spi")
    _validate_monthly_period(
        result,
        settings.spi_application_start,
        settings.spi_application_end,
        "application",
    )
    auxiliary_variables = ["number_of_zeros", "number_of_notnull", "prob_of_zero"]
    return result.drop_vars([name for name in auxiliary_variables if name in result.coords])


def build_output_dataset(
    spi: xr.DataArray,
    settings: SPISettings,
    *,
    source_metadata: SPISourceMetadata,
) -> xr.Dataset:
    """Create a metadata-rich SPI output dataset.

    Parameters
    ----------
    spi
        Calculated SPI values using canonical time, lat, and lon coordinates.
    settings
        Shared SPI and metadata configuration.
    source_metadata
        Source identity, input variables, and precipitation conversion details.

    Returns
    -------
    xarray.Dataset
        SPI dataset ready for NetCDF serialization.
    """
    dataset = spi.to_dataset()
    for dimension, standard_name, axis, units in (
        ("lat", "latitude", "Y", "degrees_north"),
        ("lon", "longitude", "X", "degrees_east"),
        ("time", "time", "T", None),
    ):
        if dimension in dataset.coords:
            dataset[dimension].attrs.update(standard_name=standard_name, axis=axis)
            if units is not None:
                dataset[dimension].attrs["units"] = units
    dataset["spi"].attrs.update(
        long_name=f"Standardized Precipitation Index ({settings.spi_scale_months}-month)",
        units="1",
        description=(
            "Positive values indicate wetter-than-normal conditions; "
            "negative values indicate drier-than-normal conditions."
        ),
        fit_failure_interpretation=(
            "With complete precipitation input, NaN SPI values indicate that the selected "
            "probability distribution could not be fitted for that calendar month and grid "
            "cell. This can occur in very arid regions when the calibration sample contains "
            "too few positive precipitation values, producing non-finite or non-positive "
            "distribution parameters."
        ),
        numerical_bounds="[-8.21, 8.21]",
        numerical_bounds_interpretation=(
            "When the fitted cumulative probability is numerically equal to 0 or 1, its "
            "transformation to the standard normal distribution would produce negative or "
            "positive infinity. xclim clips these values to -8.21 or 8.21. Values at these "
            "bounds represent events beyond the numerical resolution of the fitted "
            "distribution and do not necessarily indicate a failed calibration fit."
        ),
    )
    creation_date = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    time_start = str(dataset["time"].values[0])[:10]
    time_end = str(dataset["time"].values[-1])[:10]
    latitude_min = float(dataset["lat"].min().item())
    latitude_max = float(dataset["lat"].max().item())
    longitude_min = float(dataset["lon"].min().item())
    longitude_max = float(dataset["lon"].max().item())
    dataset.attrs.update(
        title=source_metadata.title,
        summary=(
            f"{settings.spi_scale_months}-month Standardized Precipitation Index calculated "
            f"from {source_metadata.source}. Spatial domain: lat "
            f"[{latitude_min:.2f}, {latitude_max:.2f} deg], lon "
            f"[{longitude_min:.2f}, {longitude_max:.2f} deg]. Temporal coverage: "
            f"{time_start} to {time_end}. Calibration period: "
            f"{settings.spi_calibration_start.isoformat()} to "
            f"{settings.spi_calibration_end.isoformat()}."
        ),
        creation_date=creation_date,
        creator=settings.metadata_creators,
        institution=settings.metadata_institution,
        project=settings.metadata_project,
        license=settings.metadata_license,
        references=settings.metadata_references,
        code_repository=settings.metadata_repository,
        Conventions=settings.metadata_conventions,
        processing_level=settings.metadata_processing_level,
        source=source_metadata.source,
        keywords=source_metadata.keywords,
        history=(
            f"{creation_date} Computed {settings.spi_scale_months}-month SPI using "
            "xarray and xclim."
        ),
        input_variables=source_metadata.input_variables,
        input_frequency=source_metadata.input_frequency,
        calibration_period=(
            f"{settings.spi_calibration_start.isoformat()} to "
            f"{settings.spi_calibration_end.isoformat()}"
        ),
        application_period=(
            f"{settings.spi_application_start.isoformat()} to "
            f"{settings.spi_application_end.isoformat()}"
        ),
        calibration_method=(
            f"{settings.spi_distribution} distribution fitted independently for each "
            f"calendar month and grid cell using xclim {settings.spi_method}, "
            f"floc={settings.spi_floc:g}, and zero-inflated precipitation."
        ),
        compute_backend="xarray and xclim",
        spi_scale_months=settings.spi_scale_months,
        spi_distribution=settings.spi_distribution,
        spi_fitting_method=settings.spi_method,
        spi_floc=settings.spi_floc,
        precipitation_conversion=source_metadata.precipitation_conversion,
        monthly_precipitation_units="mm month-1",
    )
    return dataset


def write_output(
    dataset: xr.Dataset,
    output_path: Path,
    settings: SPISettings,
    *,
    dask_workers: int | None = None,
) -> Path:
    """Atomically write an SPI NetCDF output.

    Parameters
    ----------
    dataset
        SPI dataset to serialize.
    output_path
        Final NetCDF path. An existing file at this exact path is replaced.
    settings
        NetCDF engine, format, and compression configuration.
    dask_workers
        Threaded Dask workers used for a delayed write. An eager write is used
        when omitted.

    Returns
    -------
    pathlib.Path
        Written output path.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = output_path.with_suffix(f"{output_path.suffix}.tmp")
    encoding: dict[str, dict[str, bool | int]] = {}
    if settings.netcdf_compression:
        encoding = {"spi": {"zlib": True, "complevel": settings.netcdf_complevel}}
    try:
        if dask_workers is None:
            dataset.to_netcdf(
                temporary_path,
                engine=settings.netcdf_engine,
                format=settings.netcdf_format,
                encoding=encoding,
            )
        else:
            write = dataset.to_netcdf(
                temporary_path,
                engine=settings.netcdf_engine,
                format=settings.netcdf_format,
                encoding=encoding,
                compute=False,
            )
            write.compute(scheduler="threads", num_workers=dask_workers)
        temporary_path.replace(output_path)
    finally:
        temporary_path.unlink(missing_ok=True)
    return output_path


def _validate_monthly_period(
    data: xr.DataArray,
    start: date,
    end: date,
    period_name: str,
) -> None:
    actual = [
        (int(year), int(month))
        for year, month in zip(
            data["time"].dt.year.values.tolist(),
            data["time"].dt.month.values.tolist(),
            strict=True,
        )
    ]
    expected: list[tuple[int, int]] = []
    year, month = start.year, start.month
    while (year, month) <= (end.year, end.month):
        expected.append((year, month))
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
    if actual != expected:
        raise ValueError(f"SPI {period_name} period must contain every configured month")
