# CMIP6 calibration data

The expected source calibration is the historical daily `tasmax` ensemble mean for 1961-1990. It must provide `time`, `lat`, and `lon`, use the processing grid, and identify kelvin units as `K` or `kelvin`. The staged NetCDF calibration uses `calibration_time`, `lat`, and `lon` and stores temperature in degC.

The canonical `.env` controls the source with `CMIP6_CALIBRATION_SOURCE_TEMPLATE`, its chunks with `CMIP6_CALIBRATION_TIME_CHUNK` and `CMIP6_SPATIAL_CHUNK`, and the staged output with `CMIP6_CALIBRATION_FILE_TEMPLATE`. Source and output templates must include the required model identity placeholders. CLI identity options override only the current invocation.
