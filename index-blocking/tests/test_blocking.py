from collections.abc import Callable

import pandas as pd
import pytest

from riskclima_blocking.series_blocking_index_cmip6 import calculate_blockings as cmip6_blockings
from riskclima_blocking.series_blocking_index_era5 import calculate_blockings as era5_blockings


@pytest.mark.parametrize("calculate", [era5_blockings, cmip6_blockings])
def test_blocking_requires_three_consecutive_positive_days(
    calculate: Callable[[pd.DataFrame], pd.Series],
) -> None:
    data = pd.DataFrame(
        {
            "vort850": [1, 1, 1, -1, 1, 1, 1],
            "vort500": [1, 1, 1, 1, 1, 1, 1],
            "anomaly": [1, 1, 1, 1, 1, 1, 1],
        },
        index=pd.date_range("2020-01-01", periods=7),
    )

    result = calculate(data)

    assert result.tolist() == [True, True, True, False, True, True, True]
