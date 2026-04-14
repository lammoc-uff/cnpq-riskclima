# catalog

This directory stores the CMIP6 catalog CSV files used as inputs for the workflow.

## Required files

- ``pangeo-cmip6_aws.csv`` — AWS-based CMIP6 catalog
- ``pangeo-cmip6_google.csv`` — Google Cloud-based CMIP6 catalog

## Notes

- These files are used to compare data availability across providers.
- File names should remain consistent with the paths defined in ``scripts/src/config.py``.
- The main comparison workflow reads both catalogs before applying the project filters.
