import torch

from riskclima_xhwi.torch_ops.xhwi import torch_heatwave_index


class TestTorchHeatwaveIndex:
    def test_preserves_formula_and_thresholds(self) -> None:
        temperature = torch.tensor([33.0, 32.0, 33.0])
        humidity = torch.tensor([50.0, 50.0, 50.0])
        target = torch.tensor([0.96, 0.96, 0.95])

        result = torch_heatwave_index(
            temperature,
            humidity,
            target,
            xhwi_minimum=0.001,
        )

        expected = (torch.exp(torch.tensor(1.0)) * 50.0 / 1000.0 - 0.001) / 14.84
        torch.testing.assert_close(result, torch.tensor([expected.item(), 0.0, 0.0]))

    def test_uses_methodological_thresholds(self) -> None:
        result = torch_heatwave_index(
            torch.tensor([32.0, 33.0]),
            torch.tensor([50.0]),
            torch.tensor([0.96, 0.95]),
            xhwi_minimum=0.001,
        )

        torch.testing.assert_close(result, torch.tensor([0.0, 0.0]))
