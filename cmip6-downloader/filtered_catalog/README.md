# Filtered Catalogs

`scripts/compare_catalogs.py` writes the preferred union catalog, identity/version/coverage-aware provider-only assets, and detailed provider/version decisions here. Preferred rows include `alternate_provider` and `alternate_zstore` only when the latest version has exactly one selected asset and one cross-provider asset in the same coverage fragment. Ambiguous shards remain separate without an associated fallback, and the decision reason records that ambiguity. Output filenames and `PREFERRED_CATALOG_PATH` are required in `.env`.
