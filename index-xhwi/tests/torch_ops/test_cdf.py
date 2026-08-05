import torch

from riskclima_xhwi.torch_ops.cdf import torch_match_cdf_linear


class TestTorchMatchCdfLinear:
    def test_interpolates_empirical_cdf_and_preserves_missing_values(self) -> None:
        values = torch.tensor([5.0, 15.0, 30.0, float("nan")]).reshape(4, 1, 1)
        calibration = torch.tensor([10.0, 20.0, 30.0]).reshape(3, 1, 1)

        result = torch_match_cdf_linear(values, calibration)

        torch.testing.assert_close(
            result[:3, 0, 0], torch.tensor([0.0, 0.5, 1.0]), rtol=0.0, atol=1e-6
        )
        assert torch.isnan(result[3, 0, 0])

    def test_requires_two_finite_calibration_values(self) -> None:
        values = torch.tensor([20.0]).reshape(1, 1, 1)
        calibration = torch.tensor([10.0, float("nan")]).reshape(2, 1, 1)

        result = torch_match_cdf_linear(values, calibration)

        assert torch.isnan(result).all()
