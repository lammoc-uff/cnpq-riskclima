# CMIP6 processed data

The CMIP6 preprocessing workflow writes nine yearly predictor CSV files to `{CMIP6_INPUT_DIRECTORY}/{SOURCE_ID}/{EXPERIMENT_ID}/{YEAR}/`. The index calculation derives its scale, principal-component, and regression intermediates under `{CMIP6_INTERMEDIATES_DIRECTORY}/{SOURCE_ID}/{EXPERIMENT_ID}/{YEAR}/`.

All files below these generated paths remain outside Git.
