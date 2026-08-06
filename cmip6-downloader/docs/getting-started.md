# Getting Started

## Environment

The project requires Python 3.12, Make, and uv. From `cmip6-downloader/`:

```bash
make install
cp .env.example .env
source .venv/bin/activate
make check
```

`make install` and `make sync` install the runtime and development groups. Install notebook support only when needed with `uv sync --group notebook`.

`.env` is the canonical operational configuration. Every setting is required and `.env.example` supplies the operational values; `Settings` has no operational defaults. Every script invocation creates a new instance and reads the file again. A supplied existing CLI option overrides the corresponding field for that invocation; omitted options retain `.env` values. Lists and mappings use JSON syntax.

## Configuration

Paths are relative to the project root unless absolute. Parent traversal (`..`) is rejected.

| Keys | Purpose and `.env.example` value |
| --- | --- |
| `CATALOG_AWS_PATH`, `CATALOG_GOOGLE_PATH` | Input CSVs: `catalog/pangeo-cmip6_aws.csv` and `catalog/pangeo-cmip6_google.csv`. |
| `FILTERED_CATALOG_DIR`, `PREFERRED_CATALOG_PATH`, `DOWNLOADS_DIR` | Comparison directory, preferred union CSV, and Zarr output root. |
| `PROVIDER_PRIORITY` | Must contain `aws` and `google` exactly once, in either order; the first wins equivalent-asset ties. |
| `AWS_ANONYMOUS`, `GOOGLE_ANONYMOUS` | Anonymous provider access; both are `true`. |
| `SOURCE_IDS` | Required model list containing the six models shown in `.env.example`. |
| `EXPERIMENT_IDS` | Required experiment list: `historical`, `ssp245`, and `ssp585`. |
| `TABLE_IDS` | Required frequency/table list: `day`, `3hr`, and `Omon`. |
| `VARIABLE_IDS` | Required variable list; see `.env.example` for the complete value. |
| `GRID_LABELS` | Required grid list: `gn`, `gr`, and `gr1`. |
| `MEMBER_IDS` | Optional member filter; `[]` means every member. |
| `HISTORICAL_START`, `HISTORICAL_END` | Optional ISO date bounds; empty means an open historical interval. |
| `HISTORICAL_EXPERIMENTS` | Non-empty experiments using historical bounds: `["historical"]`. |
| `FUTURE_EXPERIMENTS` | Experiments using future bounds: `["ssp245","ssp585"]`. |
| `FUTURE_START`, `FUTURE_END` | Future interval, `2015-01-01` through `2050-12-31`. |
| `LATITUDE_MIN`, `LATITUDE_MAX` | Domain latitude bounds, `-70` and `20`. |
| `LONGITUDE_MIN`, `LONGITUDE_MAX` | Domain longitude bounds, `-120` and `-5`. |
| `SPATIAL_SUBSET` | Enable rectilinear spatial selection; value `true`. |
| `EXCLUDED_VARIABLES` | Variables excluded from spatial subsetting and kept global: `["tos"]`. |
| `CURVILINEAR_POLICY` | `keep_global` leaves 2-D grids uncropped; `reject` fails them. |
| `CALENDAR_CONVERSION`, `TARGET_CALENDAR`, `CALENDAR_ALIGN_ON` | Calendar conversion controls: `true`, `proleptic_gregorian`, and `year`. |
| `CONVERT_DATETIME_INDEX`, `DROP_DUPLICATE_TIMES` | Convert representable CFTime values and remove repeated times; both are `true`. |
| `MAX_WORKERS`, `TIME_CHUNK_SIZE` | Positive worker and time chunk limits, `4` and `5760`. `TIME_CHUNK_SIZE` always governs ensemble `time`. |
| `OPEN_CHUNKS` | Safe dimension-to-positive-size mapping passed to remote and local operational Zarr opens; `{}` uses backend chunk metadata. |
| `REMOTE_CONSOLIDATED`, `OUTPUT_CONSOLIDATED` | Read and write consolidated Zarr metadata; both are `true`. |
| `EXISTING_POLICY` | `skip`, `overwrite`, or `fail`; value `skip`. Skip validates variable and exact expected time metadata. |
| `ENSEMBLE_MODE` | `none`, `stack`, `mean`, or `both`; value `both`. |
| `ENSEMBLE_ALIGNMENT` | `inner` or `outer`; value `inner`. |
| `ENSEMBLE_DIMENSION_CHUNKS` | Safe dimension-to-positive-size overrides for ensemble output. `time` is prohibited; the example configures `member`, `lev`, and `plev` to `1`. |
| `ENSEMBLE_DEFAULT_CHUNK_SIZE` | Positive fallback ensemble chunk size for dimensions without an override; value `64`. |
| `CLEANUP_MEMBERS` | Remove multiple member stores only after every expected member and requested ensemble validates; value `true`. A sole member is invariantly preserved. |
| `MEMBER_STORE_TEMPLATE` | Plain `.zarr` filename containing exactly one `{member_id}` field; value `member-{member_id}.zarr`. |
| `AWS_ONLY_CATALOG_FILENAME`, `GOOGLE_ONLY_CATALOG_FILENAME` | Provider-only comparison output names. |
| `PROVIDER_DECISIONS_FILENAME` | Selected/discarded provider and version decision report name. |
| `GROUP_CATALOG_FILENAME`, `GROUP_LOG_FILENAME`, `GLOBAL_LOG_FILENAME` | Group catalog, group log, and complete log names. |
| `ENSEMBLE_ALL_FILENAME`, `ENSEMBLE_MEAN_FILENAME` | Stacked and mean ensemble store names. |
| `LOG_LEVEL`, `LOG_FORMAT` | Python log severity and format: `INFO` and the standard timestamp/level/logger/message format. |

Historical and future experiment lists must be non-empty, disjoint subsets of `EXPERIMENT_IDS`. Group-local catalog, log, stacked ensemble, and mean ensemble names must all differ. Filtered output names must differ from each other and from a preferred catalog in the same directory. Resolved AWS input, Google input, and preferred output paths must also be distinct.

## Catalog Resolution

Each input CSV must contain:

```text
source_id, experiment_id, table_id, variable_id, grid_label,
member_id, version, zstore
```

Rows may also provide exactly one complete pair from `time_start`/`time_end`, `start_date`/`end_date`, or `temporal_start`/`temporal_end`. Coverage values must be non-null, nonblank, parseable, and ordered from start through end; partial or multiple known pairs are rejected. The logical identity is the six CMIP6 identifiers through `member_id`; provider is not identity. Exact duplicate rows are reported and removed. Each exact coverage interval is retained as a fragment; only equivalent intervals compete by latest version and provider priority. Distinct intervals remain candidates even when they overlap partially. Different experiments and members remain distinct.

When coverage columns are absent, all rows from the latest version of the preferred available provider are retained because they may be shards. Exclusive fragments from older versions cannot be recovered deterministically without coverage metadata. Comparison deliberately performs no remote metadata requests.

AWS-only and Google-only tables compare identity, version, and coverage when available. AWS rows require `s3://` stores and Google rows require `gs://` stores. Provider/store strings are stripped before validation. Empty alternate provider/store pairs mean no alternate; a partially populated pair is rejected. The decision report records selection status, reason, provider, and equivalent alternate provider/store. If the primary store cannot be opened, the downloader tries that alternate with its own S3 or GCS mapper options before reporting an error.

Run comparison before downloading:

```bash
python scripts/compare_catalogs.py --source-ids MIROC6 CMCC-ESM2
make compare ARGS="--source-ids MIROC6 CMCC-ESM2"
```

Use `python scripts/compare_catalogs.py --help` for the limited path, filter, and provider options.

## Download Processing

The download script rejects null, blank, unsafe, path-traversing, glob-like, unsupported-provider, and invalid-store catalog values before starting threads. All rows for one group/member become one task, and every resolved output must remain below `DOWNLOADS_DIR`.

Experiments in `HISTORICAL_EXPERIMENTS` use historical bounds; empty bounds mean all available data. Experiments in `FUTURE_EXPERIMENTS` use the configured future period. Periods are never inferred from the first timestamp. A `time` coordinate must already exist. Only `latitude` and `longitude` aliases are renamed to `lat` and `lon`; longitudes are normalized to `[-180, 180]`. Rectilinear data is subset to the configured domain. Calendar conversion, datetime conversion, and duplicate removal follow `.env`.

Member fragments are processed newest to oldest. Newer versions win duplicate timestamps while timestamps exclusive to older versions remain; `DROP_DUPLICATE_TIMES` independently controls duplicates inside each source dataset.

Outputs are invariantly Zarr v2 stores; the format is not configurable through `.env` or CLI. Each write holds a destination lock, writes to a unique sibling `.partial-<uuid>` store, consolidates metadata, reopens and validates variable/time coverage, and only then promotes it. Overwrite retains the old destination until validation and rolls it back if promotion fails. Partial stores never satisfy skip.

An ensemble is attempted only when every expected task is `Success` or `Skipped` and every member store validates. Ensemble writes are also atomic and record member IDs, member count, and time coverage. A preexisting ensemble accepted by skip must match that provenance exactly. Any member or ensemble error preserves all member stores. After complete validation, `CLEANUP_MEMBERS=true` removes multiple members; `false` preserves them, and a single member always returns before ensemble or cleanup.

```bash
python scripts/run_download.py --max-workers 2 --time-chunk-size 1440
make download ARGS="--max-workers 2 --no-cleanup-members"
make all
```

Use `python scripts/run_download.py --help` for filter, period, domain, worker, chunk, policy, ensemble, and cleanup overrides.

## Containers

The CPU-only image uses Python 3.12, a frozen uv environment, and a non-root runtime user. `/app`, UID `10001`, and the download-script entrypoint are container invariants rather than `Settings` fields. It omits catalogs, downloads, filtered outputs, notebooks, credentials, docs, and tests from the runtime build context.

```bash
make docker-build
docker run --rm cmip6-downloader:latest --help
make docker-test
```

Mount `.env`, `catalog/`, `filtered_catalog/`, and `downloads/` when running real jobs. See [Apptainer](apptainer.md) for conversion and bind examples.

## Troubleshooting

- Missing catalog columns: compare the CSV header with the required schema above.
- Missing preferred catalog: run comparison first or set `PREFERRED_CATALOG_PATH`.
- Invalid existing store: remove it or rerun with `--existing-policy overwrite` after confirming replacement is intended.
- Authentication failure: keep anonymous access enabled for public CMIP6 stores or configure provider credentials using the standard S3/GCS environment.
- Empty output: check all filter lists, `EXCLUDED_VARIABLES`, and configured periods.
- Curvilinear rejection: select `keep_global` to retain the complete grid; no rectangular crop is attempted.
- Calendar conversion error: disable datetime-index conversion if converted dates cannot be represented by pandas timestamps.
- Notebook behavior: notebooks are English-only exploratory material, derive paths from a root or `notebooks/` working directory, and are not part of the operational scripts.
