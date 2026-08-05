# ERA5-Land XHWI notebook

`xhwi_era5land_monthly_colab_torch.ipynb` is a self-contained Colab workflow for computing monthly accumulated XHWI from ERA5-Land ARCO data. It installs its runtime dependencies and does not require the local RiskClima package.

## Open and run

Open the notebook from GitHub with **Open in Colab**, or download it and use **File > Upload notebook** in Colab. Run the cells from top to bottom with **Runtime > Run all** after completing the setup below.

A GPU runtime is optional. Select one with **Runtime > Change runtime type** to accelerate PyTorch operations; the notebook falls back to CPU when CUDA is unavailable.

## Setup

1. In the first cell, leave `USE_GOOGLE_DRIVE = False` to use Colab local storage at `/content/riskclima`.
2. To keep outputs in Drive, set `USE_GOOGLE_DRIVE = True`. The notebook mounts Drive only in that case and uses `/content/drive/MyDrive/riskclima`.
3. Edit `BASE_DIR` in that cell if another neutral location is required.
4. Add `CDSAPI_KEY` under **Colab > Secrets** and grant the notebook access. Alternatively, set the `CDSAPI_KEY` environment variable for the current runtime. Do not place the key in the notebook or save it to Drive.

The default configuration processes months 1, 2, 3, 9, 10, 11, and 12 for 2010-2025 over the configured spatial slice. The calibration data are computed from the 1961-1990 period. Large runs can exceed Colab runtime or storage limits; block sizes and the selected period are editable in the configuration cell without changing the formulas.

## Outputs

Files are written below `BASE_DIR/ERA5_LAND/monthly`:

- `parts/xhwi_era5land_month_XX.nc` or `parts/xhwi_era5land_months_XX-YY.nc` for selected-month parts
- `xhwi_era5land_monthly_ind_prod.nc` for the concatenated monthly product
- `xhwi_era5land_calib_t2m_max_1961-1990.nc` when the optional one-time calibration output is written

The notebook and the package scripts are two maintained execution surfaces. Some scientific logic is intentionally duplicated so the notebook remains self-contained; changes may need to be applied to both surfaces.
