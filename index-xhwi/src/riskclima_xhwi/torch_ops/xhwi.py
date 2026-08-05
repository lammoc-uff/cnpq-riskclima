import torch


def torch_heatwave_index(
    tas_c: torch.Tensor,
    hurs: torch.Tensor,
    target: torch.Tensor,
    *,
    xhwi_minimum: float,
) -> torch.Tensor:
    """Calculate the hourly Extreme Heatwave Index.

    Parameters
    ----------
    tas_c
        Air temperature in degrees Celsius.
    hurs
        Relative humidity in percent.
    target
        Empirical CDF probability from zero to one.
    Returns
    -------
    torch.Tensor
        Hourly XHWI values.
    """
    target100 = target * 100.0
    tpe = torch.clamp(target100 - 95.0, min=0.0)
    coef = (torch.exp(tpe) * hurs) / 1000.0
    xhwi = (coef - 0.001) / 14.84
    xhwi = torch.where(tpe > 0, xhwi, torch.zeros_like(xhwi))
    xhwi = torch.where(tas_c > 32.0, xhwi, torch.zeros_like(xhwi))
    xhwi = torch.where(xhwi > xhwi_minimum, xhwi, torch.zeros_like(xhwi))
    return xhwi
