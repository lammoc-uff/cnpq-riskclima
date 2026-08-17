# ERA5 processed data

The ERA5 preprocessing workflow writes nine yearly predictor CSV files to `{ERA5_INPUT_DIRECTORY}/{YEAR}/`. The index calculation derives its scale, principal-component, and regression intermediates under `{ERA5_INTERMEDIATES_DIRECTORY}/{YEAR}/`.

All files below these generated paths remain outside Git.
