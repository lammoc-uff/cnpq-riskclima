# ERA5 XHWI ARCO Torch Workflow

This directory contains the script-based version of `xhwi_era5_monthly_colab_torch.ipynb`.

The workflow uses the ERA5 ARCO Zarr store, computes a reusable calibration file from hourly `t2m`, processes selected calendar months with PyTorch blockwise operations, and concatenates monthly part files into one final NetCDF.

## Structure

```text
new/
├── raw_data/
├── results/monthly/parts/
└── scripts/
    ├── make_calibration.py
    ├── run_months.py
    ├── concat_months.py
    ├── run_all.py
    └── src/
```

## Configuration

Edit all operational settings in:

```text
scripts/src/config/settings.py
```

This includes domain slices, months to process, calibration path, output paths, thresholds, and block sizes.

## Commands

Run from `index-xhwi/era5/new/scripts`:

```bash
python make_calibration.py
python run_months.py
python concat_months.py
```

Or run the full workflow:

```bash
python run_all.py
```
