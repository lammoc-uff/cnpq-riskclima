# ERA5 raw precipitation data

This directory stores retained ERA5 monthly total-precipitation NetCDF files acquired by the SPI workflow. The default path is:

```text
era5_tp_monthly_1940-01-01_2026-07-01.nc
```

`ERA5_RAW_FILE_TEMPLATE`, `ERA5_DOWNLOAD_START`, and `ERA5_DOWNLOAD_END` control the path. Every run downloads and validates the configured period, then atomically overwrites the exact final path. Different periods naturally use different date-based filenames, so retained `.nc` files at other paths are not removed. Temporary `.part.nc` and `.nc.tmp` files are cleaned after success or failure.

Climate data under `raw_data` are ignored by Git. This README is retained to document the directory; do not commit downloaded `.nc`, temporary `.part.nc`, or `.nc.tmp` files.
