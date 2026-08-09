import json
from pathlib import Path

import pytest

NOTEBOOK_PATHS = (
    Path("cmip6/notebooks/spi.ipynb"),
    Path("era5/notebooks/spi.ipynb"),
)


@pytest.mark.parametrize("relative_path", NOTEBOOK_PATHS, ids=lambda path: path.parts[0])
def test_spi_notebook_has_no_execution_outputs_or_personal_paths(relative_path: Path) -> None:
    notebook_path = Path(__file__).parents[1] / relative_path
    notebook_text = notebook_path.read_text(encoding="utf-8")
    notebook = json.loads(notebook_text)
    code_cells = [cell for cell in notebook["cells"] if cell["cell_type"] == "code"]

    assert all(cell["execution_count"] is None for cell in code_cells)
    assert all(cell["outputs"] == [] for cell in code_cells)
    assert "drive.mount" not in notebook_text
    assert "My Drive" not in notebook_text
    assert "input(" not in notebook_text

    python_source = "\n".join(
        "".join(cell["source"])
        for cell in code_cells
        if not "".join(cell["source"]).lstrip().startswith("!")
    )
    compile(python_source, str(relative_path), "exec")


def test_era5_notebook_uses_monthly_mean_accumulation_contract() -> None:
    notebook_text = (Path(__file__).parents[1] / "era5/notebooks/spi.ipynb").read_text(
        encoding="utf-8"
    )

    assert "days_in_month" in notebook_text
    assert "tp * 1000" in notebook_text
    assert "86400" not in notebook_text
    assert ".part.nc" in notebook_text
    assert 'join=\\"exact\\"' in notebook_text


def test_cmip6_notebook_records_identity_and_validates_grid() -> None:
    notebook_text = (Path(__file__).parents[1] / "cmip6/notebooks/spi.ipynb").read_text(
        encoding="utf-8"
    )

    for value in ("model_id", "member_id", "grid_label", "exactly equal latitude"):
        assert value in notebook_text


@pytest.mark.parametrize("relative_path", NOTEBOOK_PATHS, ids=lambda path: path.parts[0])
def test_spi_notebook_applies_shared_brazil_mask(relative_path: Path) -> None:
    notebook_text = (Path(__file__).parents[1] / relative_path).read_text(encoding="utf-8")

    for value in (
        "SPI_USE_SHAPEFILE",
        "BR_Pais_2025.shp",
        "select_spatial_domain",
        "covers",
        "spatial_mask_applied",
        "Andes",
        "Atacama Desert",
    ):
        assert value in notebook_text


def test_era5_notebook_derives_cds_area_from_shapefile_bounds() -> None:
    notebook_text = (Path(__file__).parents[1] / "era5/notebooks/spi.ipynb").read_text(
        encoding="utf-8"
    )

    assert "AREA = [north, west, south, east]" in notebook_text
    assert "monthly_precipitation = select_spatial_domain" in notebook_text
