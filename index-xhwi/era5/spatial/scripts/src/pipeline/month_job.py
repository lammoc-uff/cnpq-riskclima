import os
import xarray as xr
from src.utils.encoding import clear_chunks_encoding
from src.io.writers import to_zarr
from src.cdf.cdf import compute_sorted_and_cdf, match_cdf
from src.features.xhwi import heatwave_index
from src.features.aggregations import daily_indicators, monthly_indicators
from src.config.settings import CHUNKS, WRITE_CONSOLIDATED, OVERWRITE
from src.config.settings import RESULT_XHWI, RESULT_XHWI_DIARY_INDPROD, RESULT_XHWI_MONTH_INDPROD
from src.utils.logger import get_logger

logger = get_logger("month_job")


def process_single_month(
                         month_idx: int,
                         calib_ds: xr.Dataset,
                         val_ds: xr.Dataset,
                         out_path_hourly=RESULT_XHWI,
                         out_path_diary=RESULT_XHWI_DIARY_INDPROD,
                         out_path_monthly=RESULT_XHWI_MONTH_INDPROD,
                        ):
    """
    Process a single month of data: compute Target → XHWI → indicators → export.

    Parameters
    ----------
    month_idx : int
        Month number (1–12).
    calib_ds : xr.Dataset
        Calibration dataset (e.g., 1960–1990).
    val_ds : xr.Dataset
        Validation dataset (e.g., 1950–2024).
    out_path_hourly : Path or str
        Output path for hourly XHWI dataset.
    out_path_diary : Path or str
        Output path for daily indicators dataset.
    out_path_monthly : Path or str
        Output path for monthly indicators dataset.
    """

    logger.info(f"🚀 Starting month {month_idx}")

    # 0️⃣ Format paths dynamically based on month index
    out_path_hourly = str(out_path_hourly).format(m=month_idx)
    out_path_diary = str(out_path_diary).format(m=month_idx)
    out_path_monthly = str(out_path_monthly).format(m=month_idx)

    # 1️⃣ Subset calibration and validation datasets for this month
    cal_m = calib_ds.sel(time=calib_ds["time.month"] == month_idx)
    val_m = val_ds.sel(time=val_ds["time.month"] == month_idx)

    if cal_m.time.size == 0 or val_m.time.size == 0:
        logger.warning(f"⚠️ No data found for month {month_idx}. Skipping.")
        return

    # 2️⃣ Compute sorted calibration data and its CDF
    logger.info(f"→ Computing sorted temperature and CDF for month {month_idx}")
    sorted_da, cdf_da = compute_sorted_and_cdf(cal_m["VAR_2T"])

    cal_m = cal_m.rename({"time": "calibration_time"})
    cal_m["sorted_temp"] = sorted_da
    cal_m["t2m_max_cdf"] = cdf_da

    # 3️⃣ Interpolate validation temperature into calibration CDF
    logger.info(f"→ Matching validation CDF for month {month_idx}")
    target = match_cdf(val_m["t2m"], cal_m["sorted_temp"], cal_m["t2m_max_cdf"])
    val_m["Target"] = target

    # 4️⃣ Compute XHWI
    logger.info(f"→ Calculating XHWI for month {month_idx}")
    val_m = heatwave_index(val_m)

    # 5️⃣ Clear encodings and rechunk for export
    val_m = clear_chunks_encoding(val_m).chunk(CHUNKS)

    # 6️⃣ Write hourly XHWI (raw output)
    if OVERWRITE or not os.path.exists(out_path_hourly):
        logger.info(f"💾 Writing hourly XHWI Zarr → {out_path_hourly}")
        to_zarr(val_m, out_path_hourly, mode="w", consolidated=WRITE_CONSOLIDATED)

    # 7️⃣ Compute daily and monthly indicators
    logger.info(f"→ Aggregating daily and monthly indicators for month {month_idx}")
    ds_daily = daily_indicators(val_m)
    ds_monthly = monthly_indicators(val_m)

    # 8️⃣ Prepare outputs for diary and monthly
    ds_daily = clear_chunks_encoding(ds_daily).chunk(CHUNKS)
    ds_monthly = clear_chunks_encoding(ds_monthly).chunk(CHUNKS)

    # 9️⃣ Write daily indicator Zarr
    if OVERWRITE or not os.path.exists(out_path_diary):
        logger.info(f"💾 Writing daily indicators Zarr → {out_path_diary}")
        to_zarr(ds_daily, out_path_diary, mode="w", consolidated=WRITE_CONSOLIDATED)

    # 🔟 Write monthly indicator Zarr
    if OVERWRITE or not os.path.exists(out_path_monthly):
        logger.info(f"💾 Writing monthly indicators Zarr → {out_path_monthly}")
        to_zarr(ds_monthly, out_path_monthly, mode="w", consolidated=WRITE_CONSOLIDATED)

    logger.info(f"✅ Finished month {month_idx}\n")
