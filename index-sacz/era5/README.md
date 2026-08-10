# ERA5 SACZ workflow

The ERA5 workflow downloads pressure-level fields from the public ARCO ERA5 Google Cloud bucket, computes daily atmospheric predictors, aggregates them into yearly files, calculates spatial means over the SACZ areas, and calculates the regional SACZ index.

Run the stages independently from `index-sacz`:

```bash
make process-era5
make index-era5
```

Set `ERA5_YEAR` in `.env` or override it with `ARGS="--era5-year 2021"`. The preprocessing requires CDO and `gsutil`, as described in the main README.
