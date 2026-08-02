from dask.diagnostics import ProgressBar
import numpy as np

from src.config.settings import CALIB_OUT, ERA5_ZARR_PATH
from src.pipeline.data_access import open_calibration_tasmax_from_t2m, open_t2mcalib_inputs


def main() -> None:
    print("Opening ERA5 t2m calibration input...")
    tas_c = open_t2mcalib_inputs(ERA5_ZARR_PATH)

    print("Computing daily maximum t2m calibration field...")
    tasmax_calibration = open_calibration_tasmax_from_t2m(tas_c)
    tasmax_calibration.name = "tasmax_calibration"
    tasmax_calibration.attrs.update(
        {
            "long_name": "Daily maximum 2 m temperature for XHWI calibration",
            "units": "degC",
            "source_variable": "t2m",
            "calculation": "daily maximum from hourly ERA5 t2m",
        }
    )

    encoding = {
        "tasmax_calibration": {
            "zlib": True,
            "complevel": 4,
            "_FillValue": np.float32(np.nan),
            "dtype": "float32",
        }
    }

    if CALIB_OUT.exists():
        CALIB_OUT.unlink()

    print(f"Writing calibration file to: {CALIB_OUT}")
    with ProgressBar():
        tasmax_calibration.to_netcdf(CALIB_OUT, engine="netcdf4", encoding=encoding)

    print("Calibration file written:")
    print(CALIB_OUT)


if __name__ == "__main__":
    main()
