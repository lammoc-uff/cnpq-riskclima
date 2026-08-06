"""Settings and CLI override tests."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from src.config import Settings, settings_with_overrides

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _env_keys(path: Path) -> set[str]:
    return {
        line.split("=", maxsplit=1)[0]
        for line in path.read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#")
    }


def test_env_example_exactly_matches_settings() -> None:
    aliases = {str(field.alias) for field in Settings.model_fields.values()}
    assert _env_keys(PROJECT_ROOT / ".env.example") == aliases
    from_example = Settings(_env_file=PROJECT_ROOT / ".env.example")
    assert from_example.historical_start is None
    assert from_example.historical_end is None
    assert all(field.is_required() for field in Settings.model_fields.values())
    with pytest.raises(ValidationError, match="Field required"):
        Settings(_env_file=None)


def test_settings_rereads_env(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    example = (PROJECT_ROOT / ".env.example").read_text(encoding="utf-8")
    env_file.write_text(example.replace("MAX_WORKERS=4", "MAX_WORKERS=2"), encoding="utf-8")
    assert Settings(_env_file=env_file).max_workers == 2
    env_file.write_text(example.replace("MAX_WORKERS=4", "MAX_WORKERS=7"), encoding="utf-8")
    assert Settings(_env_file=env_file).max_workers == 7


def test_provider_priority_accepts_both_orders(settings: Settings) -> None:
    values = settings.model_dump()
    values["provider_priority"] = ["google", "aws"]
    assert Settings.model_validate(values).provider_priority == ["google", "aws"]
    values["provider_priority"] = ["aws", "aws"]
    with pytest.raises(ValidationError, match="exactly once"):
        Settings.model_validate(values)


def test_cli_override_changes_only_explicit_field(settings: Settings) -> None:
    overridden = settings_with_overrides(
        settings,
        {"max_workers": 1, "source_ids": None},
    )
    assert overridden.max_workers == 1
    assert overridden.source_ids == settings.source_ids
    assert overridden.downloads_dir == settings.downloads_dir


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("open_chunks", {"time/path": 1}, "safe identifiers"),
        ("open_chunks", {"lat": 0}, "greater than 0"),
        ("ensemble_dimension_chunks", {"time": 2}, "cannot contain time"),
        ("ensemble_default_chunk_size", 0, "greater than 0"),
    ],
)
def test_chunk_configuration_validation(
    settings: Settings,
    field: str,
    value: object,
    message: str,
) -> None:
    values = settings.model_dump()
    values[field] = value
    with pytest.raises(ValidationError, match=message):
        Settings.model_validate(values)


@pytest.mark.parametrize(
    "template",
    [
        "member.zarr",
        "{member_id}-{member_id}.zarr",
        "{other}.zarr",
        "{member_id!r}.zarr",
        "{member_id:>20}.zarr",
        "directory/{member_id}.zarr",
        "../{member_id}.zarr",
        "{member_id}.nc",
    ],
)
def test_member_store_template_validation(settings: Settings, template: str) -> None:
    values = settings.model_dump()
    values["member_store_template"] = template
    with pytest.raises(ValidationError, match="MEMBER_STORE_TEMPLATE"):
        Settings.model_validate(values)


def test_format_member_store_validates_member_id(settings: Settings) -> None:
    assert settings.format_member_store("r1i1p1f1") == "member-r1i1p1f1.zarr"
    with pytest.raises(ValueError, match="unsafe member"):
        settings.format_member_store("../r1")


@pytest.mark.parametrize(
    ("updates", "message"),
    [
        ({"historical_experiments": ["missing"]}, "subset"),
        ({"future_experiments": ["missing"]}, "subset"),
        ({"historical_experiments": ["historical", "ssp245"]}, "disjoint"),
        ({"group_log_filename": "catalog_group.csv"}, "group catalog"),
        ({"google_only_catalog_filename": "catalog_aws_only.csv"}, "filtered catalog"),
        ({"preferred_catalog_path": "filtered_catalog/catalog_aws_only.csv"}, "collides"),
        ({"preferred_catalog_path": "catalog/pangeo-cmip6_aws.csv"}, "paths"),
        ({"catalog_google_path": "catalog/pangeo-cmip6_aws.csv"}, "paths"),
    ],
)
def test_cross_field_invariants(
    settings: Settings,
    updates: dict[str, object],
    message: str,
) -> None:
    values = settings.model_dump()
    values.update(updates)
    with pytest.raises(ValidationError, match=message):
        Settings.model_validate(values)
