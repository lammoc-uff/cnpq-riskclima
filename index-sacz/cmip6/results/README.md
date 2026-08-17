# CMIP6 results

The CMIP6 index calculation writes `{CMIP6_OUTPUT_DIRECTORY}/{SOURCE_ID}/{EXPERIMENT_ID}/{YEAR}/{AB,C,DE}.csv`. These daily regional results are generated from the CMIP6 predictors and versioned model coefficients and are not committed to Git.

`make consolidate-cmip6` combines all complete numeric year directories for the configured source and experiment into `SACZ_index_daily_all_years.csv`. `make monthly-cmip6` then writes `SACZ_index_monthly_all_years.csv`, containing the sum of each regional daily index by calendar month.
