# Blocking index workflow

The workflow calculates regional daily atmospheric blocking indicators from three predictors:

- relative vorticity at 850 hPa;
- relative vorticity at 500 hPa;
- 500 hPa geopotential or geopotential-height anomaly relative to a monthly climatology.

A day is positive when all three predictors are positive. Three consecutive positive days classify the event as blocking, including the initial days once the persistence threshold is reached.

ERA5 uses a 1991–2020 reference climatology by default. CMIP6 uses a model-specific historical climatology from 1980–2010 by default. The daily workflows must use the climatology generated for the same source and configured label.

ERA5 fields are requested from CDS and processed with CDO. CMIP6 fields are read from local Zarr stores. The geographic areas are rectangular and are defined in the workflow modules.
