# CMIP6 SACZ workflow

The CMIP6 workflow reads local Zarr stores, selects the atmospheric fields and pressure levels required by the SACZ model, computes divergence and vorticity, applies the SACZ area polygons, and writes predictor CSV files.

Run the stages independently from `index-sacz`:

```bash
make process-cmip6
make index-cmip6
make consolidate-cmip6
make monthly-cmip6
```

The source, experiment, years, data path, and output paths are configured in `.env` and may be overridden with `ARGS`. CMIP6 data must follow the Zarr structure documented in the main README.

Consolidation combines every complete annual result for the configured source and experiment. Monthly aggregation sums the consolidated daily indexes by calendar month.
