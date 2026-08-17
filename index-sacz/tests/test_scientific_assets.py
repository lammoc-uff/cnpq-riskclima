from hashlib import file_digest
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest

from riskclima_sacz.libs.era5 import feature_collection

PROJECT_DIRECTORY = Path(__file__).parents[1]

SCIENTIFIC_ASSETS = (
    (
        "areas/sams_index_calc_areas.shp",
        "1fe3d56d6bca028b6e18a7cd8a109bfb02ddcd288ee3b9258b5ae9cfe249495c",
    ),
    (
        "areas/sams_index_calc_areas.shx",
        "096f7e7c78c20cdc0737cd2634cb672a8dc0e19cfbe6a9888b38737c352391e6",
    ),
    (
        "areas/sams_index_calc_areas.dbf",
        "7c5291f2d46e06f7f73eae66bcfc832c7f61ee797f36f9d3494c0318f945c335",
    ),
    (
        "areas/sams_index_calc_areas.cpg",
        "09fc313075748ce8ead962229ed89c919d5a9ff71974ee725a0b48bb87d975a2",
    ),
    (
        "coefs/step1/scale_coefs_AB.csv",
        "47066145e860075a05d7a3a3a9ef541e82e1d86bbdbe33f752db2b781808a564",
    ),
    (
        "coefs/step1/scale_coefs_C.csv",
        "874920d3de1be1f41927bc3f1eb9c42672dff965d13f1dea8bbe1d38135616d4",
    ),
    (
        "coefs/step1/scale_coefs_DE.csv",
        "54152c2b5cddcc55dc6a000c1dd12cc4b9679548f53ef0a7189a5601a23a72c9",
    ),
    (
        "coefs/step2/pc_weights_AB.csv",
        "52a48878cf9000d4e7daced4f86487d6ef9d6b36477b989f4984594abf335b83",
    ),
    (
        "coefs/step2/pc_weights_C.csv",
        "7af8e51aaaaab3708a690d69326cccd597a4f2b4932872f84e9a4d1000022ef2",
    ),
    (
        "coefs/step2/pc_weights_DE.csv",
        "33019b4cbc417ba02eda9dff9bafc6341f5bfcd913378494e533ceecde085751",
    ),
    (
        "coefs/step3/betas_AB.csv",
        "3f67dc1e8f96cdbe24aa05fcb695ac2a7395b4af51fbcb8b569348d4895efb7e",
    ),
    (
        "coefs/step3/betas_C.csv",
        "8aea8c096b84e06defa78da4a5a4cc2b92fb69ad99585b6e7df1588d846d6398",
    ),
    (
        "coefs/step3/betas_DE.csv",
        "f25b894412dfef4afb8b99a02996d889247e63bd486d119bf570e7b2839e2c58",
    ),
)


@pytest.mark.parametrize(("relative_path", "expected_digest"), SCIENTIFIC_ASSETS)
def test_scientific_asset_matches_source(relative_path: str, expected_digest: str) -> None:
    with (PROJECT_DIRECTORY / relative_path).open("rb") as asset:
        digest = file_digest(asset, "sha256").hexdigest()

    assert digest == expected_digest


def test_area_bundle_matches_predictor_catalog() -> None:
    areas = gpd.read_file(PROJECT_DIRECTORY / "areas/sams_index_calc_areas.shp")
    expected_area_ids = {
        feature.shape for collection in feature_collection for feature in collection.features
    }

    assert list(areas.columns) == ["area", "geometry"]
    assert len(areas) == 69
    assert areas.crs is None
    assert areas["area"].is_unique
    assert set(areas["area"]) == expected_area_ids
    assert areas.geometry.geom_type.eq("Polygon").all()
    assert areas.geometry.is_valid.all()
    assert not areas.geometry.is_empty.any()


@pytest.mark.parametrize(
    ("region", "predictor_count", "beta_count"),
    (("AB", 22, 6), ("C", 23, 9), ("DE", 24, 9)),
)
def test_coefficient_tables_have_matching_contracts(
    region: str, predictor_count: int, beta_count: int
) -> None:
    coefficients_directory = PROJECT_DIRECTORY / "coefs"
    scale = pd.read_csv(coefficients_directory / f"step1/scale_coefs_{region}.csv")
    weights = pd.read_csv(coefficients_directory / f"step2/pc_weights_{region}.csv")
    betas = pd.read_csv(coefficients_directory / f"step3/betas_{region}.csv")

    assert list(scale.columns) == ["var", "min", "max", "mean"]
    assert len(scale) == predictor_count
    assert scale["var"].is_unique
    assert (scale["min"] != scale["max"]).all()
    assert np.isfinite(scale[["min", "max", "mean"]].to_numpy()).all()

    assert weights.columns[0] == "PC"
    assert weights["PC"].tolist() == list(range(1, 16))
    assert weights.columns[1:].tolist() == scale["var"].tolist()
    assert np.isfinite(weights.iloc[:, 1:].to_numpy()).all()

    assert list(betas.columns) == ["PC", "beta"]
    assert betas["PC"].tolist() == list(range(1, beta_count + 1))
    assert np.isfinite(betas["beta"].to_numpy()).all()
