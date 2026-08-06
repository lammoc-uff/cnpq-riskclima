# Catalog Inputs

Place the AWS and Google CMIP6 CSV catalogs here. Their required `.env` paths are configured by `CATALOG_AWS_PATH` and `CATALOG_GOOGLE_PATH`; `.env.example` points to `pangeo-cmip6_aws.csv` and `pangeo-cmip6_google.csv`.

Both files must contain nonblank `source_id`, `experiment_id`, `table_id`, `variable_id`, `grid_label`, `member_id`, `version`, and `zstore` values. See `docs/getting-started.md` for optional temporal coverage columns and the no-coverage limitation.
