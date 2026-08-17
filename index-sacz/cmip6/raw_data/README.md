# CMIP6 raw data

CMIP6 Zarr stores are external inputs and must exist below `{CMIP6_DATA_DIRECTORY}/{SOURCE_ID}/{EXPERIMENT_ID}/day/` before preprocessing. The workflow reads `ua`, `va`, `wap`, and `zg` stores produced by the CMIP6 data acquisition workflow.

Unlike the ERA5 workflow, `riskclima-sacz-process-cmip6` does not download these source data. Climate stores are not committed to Git.
