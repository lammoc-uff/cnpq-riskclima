# scripts/compare_catalogs.py

from pathlib import Path
from src.compare import compare_cmip6_catalogs
from src.config import (
    catalog_aws,
    catalog_google,
    download_dir,
    source_ids,
    experiment_ids,
    table_ids,
    variable_ids,
)


def main():
    compare_cmip6_catalogs(
        aws_path=catalog_aws,
        google_path=catalog_google,
        output_dir=download_dir,
        source_ids=source_ids,
        experiment_ids=experiment_ids,
        table_ids=table_ids,
        variable_ids=variable_ids
    )


if __name__ == "__main__":
    main()
