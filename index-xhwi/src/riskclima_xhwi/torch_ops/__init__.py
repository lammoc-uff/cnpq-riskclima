"""PyTorch scientific operations."""

from riskclima_xhwi.torch_ops.aggregations import torch_monthly_accumulated_xhwi
from riskclima_xhwi.torch_ops.cdf import torch_match_cdf_linear
from riskclima_xhwi.torch_ops.xhwi import torch_heatwave_index

__all__ = [
    "torch_heatwave_index",
    "torch_match_cdf_linear",
    "torch_monthly_accumulated_xhwi",
]
