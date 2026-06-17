import xarray as xr
import numpy as np

# variáveis descartáveis caso apareçam no Dataset
DROP_AFTER_XHWI = ["Coef", "tpe", "Target100", "Target", "cdf", "sorted_temp", "t2m_max_cdf"]

# alvo ~200 MB/var em float32 no seu grid 163x165 → time=1944 (=81*24)
TIME_CHUNK = 1944
WIN_DAY = 24  # 24 horas → 1 dia

def daily_indicators(ds: xr.Dataset) -> xr.Dataset:
    """
    Gera agregações DIÁRIAS a partir de dados horários/sub-diários.
    - Diary_Ind_Prod = (#horas com XHWI != 0) × (soma diária de XHWI)
    - t2m_max        = máximo diário de t2m
    - r_mean         = média diária de r
    """

    # trabalhe só com o necessário
    print("10")
    needed = [v for v in ["XHWI", "t2m", "r"] if v in ds]
    print("10")
    ds = ds[needed].sortby("time")
    print("10")
    # chunk: tempo em múltiplo de 24; espacial inteiro (pequeno)
    print("10")
    ds = ds.chunk({"time": TIME_CHUNK, "latitude": -1, "longitude": -1}).unify_chunks()
    print("10")
    # máscaras e agregações via coarsen (mais leve que resample p/ passo inteiro)
    print("10")
    hwi_mask = (ds["XHWI"] != 0).astype("int16")
    print("10")
    horas_hwi_dif_zero = hwi_mask.coarsen(time=WIN_DAY, boundary="trim").sum()
    sum_diary_hwi      = ds["XHWI"].coarsen(time=WIN_DAY, boundary="trim").sum()
    t2m_max            = ds["t2m"].coarsen(time=WIN_DAY, boundary="trim").max()
    r_mean             = ds["r"].coarsen(time=WIN_DAY, boundary="trim").mean()
    print("10")
    ds_daily = xr.Dataset(
        data_vars=dict(
            Diary_Ind_Prod = (horas_hwi_dif_zero * sum_diary_hwi),
            t2m_max        = t2m_max,
            r_mean         = r_mean,
        )
    )
    print("10")
    n_daily = ds_daily.sizes["time"]
    time_daily = ds.time.isel(time=slice(0, n_daily * WIN_DAY, WIN_DAY))
    ds_daily = ds_daily.assign_coords(time=time_daily)
    print("10")
    return ds_daily


def monthly_indicators(ds_hourly: xr.Dataset):
    """
    Agrega DIÁRIO → MENSAL.
    - Monthly_Ind_Prod = soma mensal de Diary_Ind_Prod
    - t2m_max          = máximo mensal de t2m_max
    - r_mean           = média mensal de r_mean
    Retorna (ds_daily, ds_monthly), ambos lazy.
    """
    ds_daily = daily_indicators(ds_hourly)
    print("10")
    ds_monthly = xr.Dataset(
        data_vars=dict(
            Monthly_Ind_Prod = ds_daily["Diary_Ind_Prod"].resample(time="MS").sum(),
            t2m_max          = ds_daily["t2m_max"].resample(time="MS").max(),
            r_mean           = ds_daily["r_mean"].resample(time="MS").mean(),
        )
    )
    print("10")
    # limpeza só se essas variáveis aparecerem (normalmente não estarão aqui)
    to_drop = [v for v in DROP_AFTER_XHWI if v in ds_monthly.variables]
    print("10")
    if to_drop:
        ds_monthly = ds_monthly.drop_vars(to_drop)
    print("10")
    #ds_monthly = ds_monthly.dropna(dim="time", how="any") descomentar porque o processo gera NA.
    #Pode ser que não tenha ram, porém
    return ds_daily, ds_monthly
