import numpy as np
import torch
import xarray as xr

from riskclima_xhwi.torch_ops.aggregations import torch_monthly_accumulated_xhwi


class TestTorchMonthlyAccumulatedXhwi:
    def test_multiplies_daily_active_hours_then_sums_by_month(self) -> None:
        time = xr.DataArray(
            np.array(
                [
                    "2001-01-01T00",
                    "2001-01-01T01",
                    "2001-01-01T02",
                    "2001-01-02T00",
                    "2001-01-02T01",
                    "2001-02-01T00",
                    "2001-02-01T01",
                ],
                dtype="datetime64[h]",
            ),
            dims="time",
        )
        values = torch.tensor([1.0, 2.0, 0.0, 3.0, 0.0, 4.0, 5.0]).reshape(7, 1, 1)

        monthly, monthly_time = torch_monthly_accumulated_xhwi(values, time)

        np.testing.assert_allclose(monthly[:, 0, 0], [9.0, 18.0])
        np.testing.assert_array_equal(
            monthly_time.astype("datetime64[h]"),
            np.array(["2001-01-01T00", "2001-02-01T00"], dtype="datetime64[h]"),
        )
