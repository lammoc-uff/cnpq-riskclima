import torch

from src.config.settings import CDF_THRESHOLD_PERCENT, TEMPERATURE_THRESHOLD_C


def torch_heatwave_index(
    tas_c: torch.Tensor,
    hurs: torch.Tensor,
    target: torch.Tensor,
) -> torch.Tensor:
    target100 = target * 100.0
    tpe = torch.clamp(target100 - CDF_THRESHOLD_PERCENT, min=0.0)
    coef = (torch.exp(tpe) * hurs) / 1000.0
    xhwi = (coef - 0.001) / 14.84

    xhwi = torch.where(tpe > 0, xhwi, torch.zeros_like(xhwi))
    xhwi = torch.where(tas_c > TEMPERATURE_THRESHOLD_C, xhwi, torch.zeros_like(xhwi))
    xhwi = torch.where(xhwi > 0.001, xhwi, torch.zeros_like(xhwi))
    return xhwi
