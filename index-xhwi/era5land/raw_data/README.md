# ERA5-Land calibration data

The expected calibration is `xhwi_era5land_calib_t2m_max_1961-1990.nc`. It contains daily maximum 2 m temperature in degC for 1961-1990, with `calibration_time`, `lat`, and `lon` dimensions. The workflow derives it from hourly `2m_temperature` using non-overlapping 24-hour maxima.

The canonical `.env` controls the path through `ERA5LAND_CALIBRATION_FILE_TEMPLATE` and the period through `ERA5LAND_CALIBRATION_START` and `ERA5LAND_CALIBRATION_END`. The template must contain `{start_year}` and `{end_year}`. CLI options only provide temporary overrides where exposed by `--help`.
