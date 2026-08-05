import json
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).parents[1]
NOTEBOOK_PATHS = (
    Path("era5/notebooks/xhwi_era5_monthly_colab_torch.ipynb"),
    Path("era5land/notebooks/xhwi_era5land_monthly_colab_torch.ipynb"),
    Path("cmip6/notebooks/xhwi_cmip6_monthly_colab_torch.ipynb"),
)


@pytest.mark.parametrize("relative_path", NOTEBOOK_PATHS, ids=lambda path: path.parts[0])
def test_operational_notebook_scientific_constants_and_clean_state(relative_path: Path) -> None:
    notebook_text = (PROJECT_ROOT / relative_path).read_text(encoding="utf-8")
    notebook = json.loads(notebook_text)

    temperature_identifier = "_".join(("TEMPERATURE", "THRESHOLD", "C"))
    cdf_identifier = "_".join(("CDF", "THRESHOLD", "PERCENT"))

    assert temperature_identifier not in notebook_text
    assert cdf_identifier not in notebook_text
    assert "target100 - 95.0" in notebook_text
    assert "tas_c > 32.0" in notebook_text
    assert "XHWI_MINIMUM = 0.001" in notebook_text
    assert "xhwi > XHWI_MINIMUM" in notebook_text

    code_cells = [cell for cell in notebook["cells"] if cell["cell_type"] == "code"]
    assert all(cell["execution_count"] is None for cell in code_cells)
    assert all(cell["outputs"] == [] for cell in code_cells)
