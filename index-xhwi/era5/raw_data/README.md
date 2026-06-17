# raw_data

This directory stores the input datasets required to run the XHWI workflow.

## Required files

- ``temp_max_Brazil_1960-1990.nc`` — calibration dataset used to compute the reference temperature distribution and empirical cumulative distribution function ``CDF`` (daily maximum temperatures of a reference period)
- ``r_prev_1950-2024/`` — directory containing ERA5 relative humidity files or the corresponding Zarr store
- ``t2m_prev_1950-2024/`` — directory containing ERA5 2 m air temperature files or the corresponding Zarr store

## Notes

- The calibration file is used as the historical baseline for probability matching.
- Temperature and humidity data are used as validation inputs for XHWI computation.
- File and directory names should remain consistent with the paths defined in ``src/config/settings.py``.
