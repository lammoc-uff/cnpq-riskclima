# ERA5 blocking workflow

The ERA5 workflow generates the 1991–2020 monthly climatology and calculates the daily blocking series for the configured period. It uses the CDS API for downloads and CDO for monthly climatologies.

Run the stages independently:

```bash
make climatology-era5
make series-era5
```
