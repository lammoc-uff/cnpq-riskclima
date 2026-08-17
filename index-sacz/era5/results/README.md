# ERA5 results

The ERA5 index calculation writes `{ERA5_OUTPUT_DIRECTORY}/{YEAR}/{AB,C,DE}.csv`. These daily regional results are generated from the ERA5 predictors and versioned model coefficients and are not committed to Git.

`make consolidate-era5` combines all complete numeric year directories into `SACZ_index_daily_all_years.csv`. `make monthly-era5` then writes `SACZ_index_monthly_all_years.csv`, containing the sum of each regional daily index by calendar month.
