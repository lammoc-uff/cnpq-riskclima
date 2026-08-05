# CMIP6 XHWI notebook

`xhwi_cmip6_monthly_colab_torch.ipynb` is a self-contained Colab workflow for computing monthly accumulated XHWI from prepared CMIP6 Zarr stores. It installs its runtime dependencies and does not require the local RiskClima package.

## Open and run

Open the notebook from GitHub with **Open in Colab**, or download it and use **File > Upload notebook** in Colab. After uploading the input stores and checking the configuration, run the cells from top to bottom with **Runtime > Run all**. The scenario cells run in this order: historical, SSP2-4.5, and SSP5-8.5; optional smoke-test and metadata-inspection cells follow them.

A GPU runtime is optional. Select one with **Runtime > Change runtime type** to accelerate PyTorch operations; the notebook falls back to CPU when CUDA is unavailable.

## Setup and paths

1. In the initial Colab setup cell, leave `USE_GOOGLE_DRIVE = False` to use local storage at `/content/riskclima`.
2. To read inputs and keep outputs in Drive, set `USE_GOOGLE_DRIVE = True`. The notebook mounts Drive only in that case and uses `/content/drive/MyDrive/riskclima`.
3. Edit `BASE_DIR` if the manually uploaded Zarr stores use another neutral location.
4. Place the configured model directory directly below `BASE_DIR`, below `BASE_DIR/cmip6`, or below `BASE_DIR/index-xhwi/cmip6`. The default model is `BCC-CSM2-MR` with member `r1i1p1f1` and grid `gn`.

Each scenario requires the `tas` and `huss` 3-hourly Zarr stores and the expected daily `tasmax` Zarr store in the paths printed by the configuration cell. This notebook does not use CDS credentials or `CDSAPI_KEY`.

## Outputs

Files are written below `<model>/results/xhwi_torch`:

- `xhwi_cmip6_BCC-CSM2-MR_historical_r1i1p1f1_monthly_accumulated_torch.nc`
- `xhwi_cmip6_BCC-CSM2-MR_ssp245_r1i1p1f1_monthly_accumulated_torch.nc`
- `xhwi_cmip6_BCC-CSM2-MR_ssp585_r1i1p1f1_monthly_accumulated_torch.nc`

Names change consistently when `MODEL_ID` or `MEMBER_ID` is edited.

The notebook and the package scripts are two maintained execution surfaces. Some scientific logic is intentionally duplicated so the notebook remains self-contained; changes may need to be applied to both surfaces.
