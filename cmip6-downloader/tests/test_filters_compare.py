"""Catalog filtering and provider-union tests."""

from pathlib import Path

import pandas as pd
import pytest

from src.compare import compare_cmip6_catalogs, resolve_provider_union
from src.config import Settings
from src.filters import (
    REQUIRED_CATALOG_COLUMNS,
    filter_catalog,
    normalize_fields,
    validate_catalog_schema,
)


def test_schema_and_filters(catalog: pd.DataFrame, settings: Settings) -> None:
    validate_catalog_schema(catalog)
    assert len(filter_catalog(catalog, settings)) == 1
    with pytest.raises(ValueError, match="member_id"):
        validate_catalog_schema(catalog.drop(columns="member_id"))


def test_aws_priority_and_exact_duplicates(
    catalog: pd.DataFrame,
    settings: Settings,
) -> None:
    aws = pd.concat([catalog, catalog], ignore_index=True)
    google = catalog.assign(zstore="gs://bucket/tas.zarr")
    preferred, decisions = resolve_provider_union(aws, google, settings)
    assert len(preferred) == 1
    assert preferred.iloc[0]["provider"] == "aws"
    assert set(decisions["status"]) == {"selected", "discarded"}
    assert "exact duplicate" in decisions["reason"].tolist()
    assert decisions.loc[decisions["status"] == "selected", "alternate_provider"].item() == "google"
    assert preferred.iloc[0]["selected_provider"] == "aws"
    assert preferred.iloc[0]["alternate_provider"] == "google"
    assert preferred.iloc[0]["alternate_zstore"] == "gs://bucket/tas.zarr"


def test_google_only_is_selected(catalog: pd.DataFrame, settings: Settings) -> None:
    google = catalog.assign(zstore="gs://bucket/tas.zarr")
    preferred, _ = resolve_provider_union(catalog.iloc[0:0], google, settings)
    assert preferred["provider"].tolist() == ["google"]


def test_latest_version_without_coverage(catalog: pd.DataFrame, settings: Settings) -> None:
    old = catalog.assign(version="v20180101")
    latest = catalog.assign(version="v20240101", zstore="s3://bucket/latest.zarr")
    preferred, decisions = resolve_provider_union(
        pd.concat([old, latest]), catalog.iloc[0:0], settings
    )
    assert preferred["version"].tolist() == ["v20240101"]
    assert "older version for equivalent coverage" in decisions["reason"].tolist()


def test_non_overlapping_segments_and_experiments_are_preserved(
    catalog: pd.DataFrame,
    settings: Settings,
) -> None:
    first = catalog.assign(time_start="1950-01-01", time_end="1970-12-31")
    second = catalog.assign(
        version="v20210101",
        zstore="s3://bucket/second.zarr",
        time_start="1971-01-01",
        time_end="2014-12-31",
    )
    future = catalog.assign(
        experiment_id="ssp245",
        zstore="s3://bucket/future.zarr",
        time_start="2015-01-01",
        time_end="2050-12-31",
    )
    preferred, _ = resolve_provider_union(
        pd.concat([first, second, future], ignore_index=True),
        catalog.iloc[0:0],
        settings,
    )
    assert len(preferred) == 3
    assert set(preferred["experiment_id"]) == {"historical", "ssp245"}


def test_broad_old_and_two_new_coverage_shards_are_retained(
    catalog: pd.DataFrame,
    settings: Settings,
) -> None:
    old = catalog.assign(
        version="v20200101",
        time_start="1950-01-01",
        time_end="2014-12-31",
    )
    new_first = catalog.assign(
        version="v20240101",
        zstore="s3://bucket/new-first.zarr",
        time_start="1950-01-01",
        time_end="1980-12-31",
    )
    new_second = catalog.assign(
        version="v20240101",
        zstore="s3://bucket/new-second.zarr",
        time_start="1981-01-01",
        time_end="2014-12-31",
    )
    preferred, _ = resolve_provider_union(
        pd.concat([old, new_first, new_second], ignore_index=True),
        catalog.iloc[0:0],
        settings,
    )
    assert set(preferred["zstore"]) == {
        "s3://bucket/tas.zarr",
        "s3://bucket/new-first.zarr",
        "s3://bucket/new-second.zarr",
    }


def test_partial_new_coverage_keeps_old_coverage_candidate(
    catalog: pd.DataFrame,
    settings: Settings,
) -> None:
    old = catalog.assign(time_start="1950-01-01", time_end="2014-12-31")
    partial = catalog.assign(
        version="v20250101",
        zstore="s3://bucket/partial.zarr",
        time_start="2000-01-01",
        time_end="2014-12-31",
    )
    preferred, _ = resolve_provider_union(pd.concat([old, partial]), catalog.iloc[0:0], settings)
    assert len(preferred) == 2


def test_same_latest_version_shards_without_coverage_are_preserved(
    catalog: pd.DataFrame,
    settings: Settings,
) -> None:
    shards = pd.concat(
        [catalog, catalog.assign(zstore="s3://bucket/second.zarr")],
        ignore_index=True,
    )
    preferred, _ = resolve_provider_union(shards, catalog.iloc[0:0], settings)
    assert preferred["zstore"].tolist() == [
        "s3://bucket/second.zarr",
        "s3://bucket/tas.zarr",
    ]


def test_multiple_shards_without_coverage_do_not_share_alternate(
    catalog: pd.DataFrame,
    settings: Settings,
) -> None:
    aws = pd.concat(
        [catalog, catalog.assign(zstore="s3://bucket/second.zarr")],
        ignore_index=True,
    )
    google = catalog.assign(zstore="gs://bucket/tas.zarr")
    preferred, decisions = resolve_provider_union(aws, google, settings)

    assert preferred["zstore"].tolist() == [
        "s3://bucket/second.zarr",
        "s3://bucket/tas.zarr",
    ]
    assert preferred["alternate_provider"].tolist() == ["", ""]
    assert preferred["alternate_zstore"].tolist() == ["", ""]
    selected_reasons = decisions.loc[decisions["status"] == "selected", "reason"]
    assert selected_reasons.str.contains("fallback not associated").all()


def test_empty_catalogs_return_schemaful_frames(
    catalog: pd.DataFrame,
    settings: Settings,
) -> None:
    empty = catalog.iloc[0:0]
    preferred, decisions = resolve_provider_union(empty, empty, settings)
    assert preferred.empty
    assert decisions.empty
    assert set(REQUIRED_CATALOG_COLUMNS) <= set(preferred.columns)
    assert {"status", "alternate_zstore", "reason"} <= set(decisions.columns)


def test_empty_catalog_comparison_writes_schemaful_csvs(
    catalog: pd.DataFrame,
    settings: Settings,
    tmp_path: Path,
) -> None:
    aws_path = tmp_path / "aws.csv"
    google_path = tmp_path / "google.csv"
    catalog.iloc[0:0].to_csv(aws_path, index=False)
    catalog.iloc[0:0].to_csv(google_path, index=False)
    values = settings.model_dump()
    values.update(
        {
            "catalog_aws_path": aws_path,
            "catalog_google_path": google_path,
            "filtered_catalog_dir": tmp_path / "filtered",
            "preferred_catalog_path": tmp_path / "filtered" / "preferred.csv",
        }
    )
    configured = Settings.model_validate(values)
    assert compare_cmip6_catalogs(configured).empty
    preferred = pd.read_csv(configured.preferred_catalog_path)
    decisions = pd.read_csv(
        configured.filtered_catalog_dir / configured.provider_decisions_filename
    )
    assert set(REQUIRED_CATALOG_COLUMNS) <= set(preferred.columns)
    assert {"status", "reason", "alternate_zstore"} <= set(decisions.columns)


@pytest.mark.parametrize(
    ("column", "value"),
    [("source_id", "../MIROC6"), ("member_id", "r1*"), ("zstore", "https://bad")],
)
def test_catalog_rejects_unsafe_values(
    catalog: pd.DataFrame,
    column: str,
    value: str,
) -> None:
    with pytest.raises(ValueError):
        validate_catalog_schema(catalog.assign(**{column: value}))


@pytest.mark.parametrize(
    ("provider", "zstore"),
    [("aws", "gs://bucket/data.zarr"), ("google", "s3://bucket/data.zarr")],
)
def test_provider_store_scheme_must_match(
    catalog: pd.DataFrame,
    provider: str,
    zstore: str,
) -> None:
    with pytest.raises(ValueError, match="does not match provider"):
        normalize_fields(catalog.assign(provider=provider, zstore=zstore))


def test_provider_spaces_are_normalized(
    catalog: pd.DataFrame,
    settings: Settings,
) -> None:
    spaced = catalog.assign(provider=" aws ", zstore=" s3://bucket/data.zarr ")
    normalized = normalize_fields(spaced)
    assert normalized.loc[0, "provider"] == "aws"
    preferred, _ = resolve_provider_union(normalized, catalog.iloc[0:0], settings)
    assert preferred.loc[0, "provider"] == "aws"


def test_unknown_provider_fails(catalog: pd.DataFrame) -> None:
    with pytest.raises(ValueError, match="unsupported provider"):
        normalize_fields(catalog.assign(provider="azure"))


def test_blank_alternate_pair_normalizes_to_no_alternate(catalog: pd.DataFrame) -> None:
    normalized = normalize_fields(
        catalog.assign(provider="aws", alternate_provider=" ", alternate_zstore=None)
    )
    assert normalized.loc[0, "alternate_provider"] == ""
    assert normalized.loc[0, "alternate_zstore"] == ""


@pytest.mark.parametrize(
    "updates",
    [
        {"alternate_provider": "google"},
        {"alternate_provider": "google", "alternate_zstore": ""},
        {"alternate_provider": "", "alternate_zstore": "gs://bucket/data.zarr"},
    ],
)
def test_incomplete_alternate_fails(catalog: pd.DataFrame, updates: dict[str, str]) -> None:
    with pytest.raises(ValueError, match="incomplete alternate"):
        normalize_fields(catalog.assign(provider="aws", **updates))


@pytest.mark.parametrize(
    ("updates", "message"),
    [
        ({"time_start": "2000-01-01"}, "incomplete coverage"),
        ({"time_start": "", "time_end": "2000-01-02"}, "blank"),
        ({"time_start": "invalid", "time_end": "2000-01-02"}, "parseable"),
        ({"time_start": "2000-01-03", "time_end": "2000-01-02"}, "after"),
        (
            {
                "time_start": "2000-01-01",
                "time_end": "2000-01-02",
                "start_date": "2000-01-01",
                "end_date": "2000-01-02",
            },
            "ambiguous",
        ),
    ],
)
def test_coverage_validation(
    catalog: pd.DataFrame,
    updates: dict[str, str],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        normalize_fields(catalog.assign(**updates))
