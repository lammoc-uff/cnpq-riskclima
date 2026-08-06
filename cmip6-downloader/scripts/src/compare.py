"""Provider catalog union and deterministic asset resolution."""

from __future__ import annotations

import logging

import pandas as pd

from src.config import Settings
from src.filters import (
    IDENTITY_COLUMNS,
    coverage_columns,
    filter_catalog,
    normalize_fields,
    version_key,
)

LOGGER = logging.getLogger(__name__)
DECISION_COLUMNS = [
    *IDENTITY_COLUMNS,
    "version",
    "zstore",
    "provider",
    "status",
    "selected_provider",
    "alternate_provider",
    "alternate_zstore",
    "reason",
]
PREFERRED_METADATA_COLUMNS = [
    "selected_provider",
    "alternate_provider",
    "alternate_zstore",
    "selection_reason",
]


def _resolve_fragment(
    fragment: pd.DataFrame,
    settings: Settings,
    preserve_shards: bool,
) -> tuple[list[int], list[dict[str, str]]]:
    latest = max(fragment["version"].astype(str), key=version_key)
    latest_rows = fragment.loc[fragment["version"].astype(str) == latest]
    selected_provider = next(
        provider
        for provider in settings.provider_priority
        if (latest_rows["provider"] == provider).any()
    )
    provider_rows = latest_rows.loc[latest_rows["provider"] == selected_provider].sort_values(
        "zstore"
    )
    selected_indices = (
        provider_rows["_resolution_index"].astype(int).tolist()
        if preserve_shards
        else [int(provider_rows.iloc[0]["_resolution_index"])]
    )
    selected_set = set(selected_indices)
    alternatives = latest_rows.loc[latest_rows["provider"] != selected_provider]
    has_unambiguous_alternate = len(provider_rows) == 1 and len(alternatives) == 1
    alternate = alternatives.iloc[0] if has_unambiguous_alternate else None
    decisions: list[dict[str, str]] = []
    for _, row in fragment.iterrows():
        resolution_index = int(row["_resolution_index"])
        selected = resolution_index in selected_set
        alternate_provider = (
            str(alternate["provider"]) if selected and alternate is not None else ""
        )
        alternate_zstore = str(alternate["zstore"]) if selected and alternate is not None else ""
        if selected and alternate_provider:
            reason = "latest version and preferred provider; equivalent alternate available"
        elif selected and not alternatives.empty:
            reason = (
                "latest version and preferred provider; fallback not associated because "
                "equivalent assets are ambiguous"
            )
        elif selected and preserve_shards:
            reason = "latest version shard from preferred available provider"
        elif selected:
            reason = "latest version for exact coverage interval and preferred provider"
        elif str(row["version"]) != latest:
            reason = "older version for equivalent coverage"
        else:
            reason = f"provider priority selected {selected_provider}"
        decisions.append(
            {
                **{column: str(row[column]) for column in IDENTITY_COLUMNS},
                "version": str(row["version"]),
                "zstore": str(row["zstore"]),
                "provider": str(row["provider"]),
                "status": "selected" if selected else "discarded",
                "selected_provider": selected_provider,
                "alternate_provider": alternate_provider,
                "alternate_zstore": alternate_zstore,
                "reason": reason,
            }
        )
    return selected_indices, decisions


def resolve_provider_union(
    aws: pd.DataFrame,
    google: pd.DataFrame,
    settings: Settings,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Resolve providers without discarding distinct temporal fragments.

    Without coverage columns, only shards from the latest version of the preferred
    available provider are retained. Exclusive fragments from older versions cannot
    be recovered deterministically when their temporal coverage is unknown.
    """
    frames: list[pd.DataFrame] = []
    for provider, frame in (("aws", aws), ("google", google)):
        filtered = filter_catalog(
            normalize_fields(frame.assign(provider=provider)), settings
        ).copy()
        frames.append(filtered)
    raw_combined = pd.concat(frames, ignore_index=True)
    duplicate_rows = raw_combined.loc[raw_combined.duplicated(keep="first")]
    combined = raw_combined.drop_duplicates().reset_index(drop=True)
    combined["_resolution_index"] = range(len(combined))
    selected_indices: list[int] = []
    decision_rows: list[dict[str, str]] = []

    for _, identity_group in combined.groupby(IDENTITY_COLUMNS, sort=False, dropna=False):
        coverage = coverage_columns(identity_group)
        fragments = (
            identity_group.groupby(list(coverage), sort=False, dropna=False)
            if coverage is not None
            else [(None, identity_group)]
        )
        for _, fragment in fragments:
            selected, decisions = _resolve_fragment(
                fragment,
                settings,
                preserve_shards=coverage is None,
            )
            selected_indices.extend(selected)
            decision_rows.extend(decisions)

    for _, row in duplicate_rows.iterrows():
        matching = next(
            (
                decision
                for decision in decision_rows
                if all(
                    decision[column] == str(row[column])
                    for column in [*IDENTITY_COLUMNS, "version", "zstore", "provider"]
                )
            ),
            None,
        )
        decision_rows.append(
            {
                **{column: str(row[column]) for column in IDENTITY_COLUMNS},
                "version": str(row["version"]),
                "zstore": str(row["zstore"]),
                "provider": str(row["provider"]),
                "status": "discarded",
                "selected_provider": matching["selected_provider"] if matching else "",
                "alternate_provider": matching["alternate_provider"] if matching else "",
                "alternate_zstore": matching["alternate_zstore"] if matching else "",
                "reason": "exact duplicate",
            }
        )

    decisions = pd.DataFrame(decision_rows, columns=DECISION_COLUMNS)
    preferred = combined.set_index("_resolution_index").loc[selected_indices].reset_index(drop=True)
    selected_decisions = decisions.loc[decisions["status"] == "selected"].rename(
        columns={"reason": "selection_reason"}
    )
    join_columns = [*IDENTITY_COLUMNS, "version", "zstore", "provider"]
    preferred = preferred.merge(
        selected_decisions[[*join_columns, *PREFERRED_METADATA_COLUMNS]],
        on=join_columns,
        how="left",
    )
    for column in PREFERRED_METADATA_COLUMNS:
        if column not in preferred:
            preferred[column] = pd.Series(dtype="string")
    return preferred, decisions


def _provider_only(
    frame: pd.DataFrame,
    other: pd.DataFrame,
) -> pd.DataFrame:
    def asset_keys(catalog: pd.DataFrame) -> list[tuple[str, ...]]:
        coverage = coverage_columns(catalog)
        keys: list[tuple[str, ...]] = []
        for _, row in catalog.iterrows():
            identity = tuple(str(row[column]) for column in [*IDENTITY_COLUMNS, "version"])
            if coverage is None:
                keys.append((*identity, "without-coverage"))
            else:
                keys.append(
                    (*identity, "with-coverage", str(row[coverage[0]]), str(row[coverage[1]]))
                )
        return keys

    other_keys = set(asset_keys(other))
    if frame.empty:
        return frame.copy()
    exclusive = [key not in other_keys for key in asset_keys(frame)]
    return frame.loc[exclusive].drop_duplicates()


def compare_cmip6_catalogs(settings: Settings) -> pd.DataFrame:
    """Read, validate, resolve, and write all catalog comparison outputs."""
    aws_path = settings.resolve_path(settings.catalog_aws_path)
    google_path = settings.resolve_path(settings.catalog_google_path)
    output_dir = settings.resolve_path(settings.filtered_catalog_dir)
    aws = normalize_fields(pd.read_csv(aws_path).assign(provider="aws"))
    google = normalize_fields(pd.read_csv(google_path).assign(provider="google"))
    filtered_aws = filter_catalog(aws, settings)
    filtered_google = filter_catalog(google, settings)
    preferred, decisions = resolve_provider_union(filtered_aws, filtered_google, settings)
    aws_only = _provider_only(filtered_aws, filtered_google)
    google_only = _provider_only(filtered_google, filtered_aws)

    output_dir.mkdir(parents=True, exist_ok=True)
    preferred_path = settings.resolve_path(settings.preferred_catalog_path)
    preferred_path.parent.mkdir(parents=True, exist_ok=True)
    preferred.to_csv(preferred_path, index=False)
    aws_only.to_csv(output_dir / settings.aws_only_catalog_filename, index=False)
    google_only.to_csv(output_dir / settings.google_only_catalog_filename, index=False)
    decisions.to_csv(output_dir / settings.provider_decisions_filename, index=False)
    LOGGER.info("Wrote %d preferred catalog assets to %s", len(preferred), preferred_path)
    return preferred
