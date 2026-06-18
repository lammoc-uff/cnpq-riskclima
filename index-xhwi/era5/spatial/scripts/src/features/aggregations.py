import xarray as xr

# Disposable variables to drop if they appear in the Dataset
DROP_AFTER_XHWI = ["Coef", "tpe", "Target100", "Target", "cdf", "sorted_temp", "t2m_max_cdf"]

# Target ~200 MB/variable in float32 on the 163x165 grid: time=1944 (=81*24)
TIME_CHUNK = 1944
WIN_DAY = 24  # 24 hours = 1 day

def daily_indicators(ds: xr.Dataset) -> xr.Dataset:
    """
    Generate daily aggregations from hourly or sub-daily data.
    - Diary_Ind_Prod = (#hours with XHWI != 0) x (daily sum of XHWI)
    - t2m_max        = daily maximum of t2m
    - r_mean         = daily mean of r
    """

    required = ["XHWI", "t2m", "r"]
    missing = [v for v in required if v not in ds]
    if missing:
        raise KeyError(f"Missing required variables for daily indicators: {missing}")

    # Work only with the variables needed for the aggregation.
    needed = required
    ds = ds[needed].sortby("time")
    # Time chunk is a multiple of 24; keep the small spatial grid whole.
    ds = ds.chunk({"time": TIME_CHUNK, "latitude": -1, "longitude": -1}).unify_chunks()
    # Use coarsen for masks and aggregations; it is lighter than resample for fixed steps.
    hwi_mask = (ds["XHWI"] != 0).astype("int16")
    hours_hwi_nonzero = hwi_mask.coarsen(time=WIN_DAY, boundary="trim").sum()
    sum_diary_hwi      = ds["XHWI"].coarsen(time=WIN_DAY, boundary="trim").sum()
    t2m_max            = ds["t2m"].coarsen(time=WIN_DAY, boundary="trim").max()
    r_mean             = ds["r"].coarsen(time=WIN_DAY, boundary="trim").mean()
    ds_daily = xr.Dataset(
        data_vars=dict(
            Diary_Ind_Prod = (hours_hwi_nonzero * sum_diary_hwi),
            t2m_max        = t2m_max,
            r_mean         = r_mean,
        )
    )
    n_daily = ds_daily.sizes["time"]
    time_daily = ds.time.isel(time=slice(0, n_daily * WIN_DAY, WIN_DAY))
    ds_daily = ds_daily.assign_coords(time=time_daily)
    return ds_daily


def monthly_indicators(ds_hourly: xr.Dataset):
    """
    Aggregate daily indicators into monthly indicators.
    - Monthly_Ind_Prod = monthly sum of Diary_Ind_Prod
    - t2m_max          = monthly maximum of t2m_max
    - r_mean           = monthly mean of r_mean
    Returns (ds_daily, ds_monthly), both lazy.
    """
    ds_daily = daily_indicators(ds_hourly)
    ds_monthly = xr.Dataset(
        data_vars=dict(
            Monthly_Ind_Prod = ds_daily["Diary_Ind_Prod"].resample(time="MS").sum(),
            t2m_max          = ds_daily["t2m_max"].resample(time="MS").max(),
            r_mean           = ds_daily["r_mean"].resample(time="MS").mean(),
        )
    )
    # Clean only if these variables appear; normally they are not present here.
    to_drop = [v for v in DROP_AFTER_XHWI if v in ds_monthly.variables]
    if to_drop:
        ds_monthly = ds_monthly.drop_vars(to_drop)
    # Uncomment if generated NaNs need to be removed and memory is sufficient.
    # ds_monthly = ds_monthly.dropna(dim="time", how="any")
    return ds_daily, ds_monthly
