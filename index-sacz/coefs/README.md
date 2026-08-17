# SACZ model coefficients

This directory stores the coefficient tables used by both ERA5 and CMIP6 index calculations.

- `step1/scale_coefs_{AB,C,DE}.csv` contains `var`, `min`, `max`, and `mean`. These values normalize each regional predictor.
- `step2/pc_weights_{AB,C,DE}.csv` contains a `PC` index from 1 to 15 and one weight column for each regional predictor.
- `step3/betas_{AB,C,DE}.csv` contains `PC` and `beta`. The first row supplies the intercept; the remaining rows supply the regional logistic-regression coefficients.

The predictor names and order in each step 1 table match the weight columns in its corresponding step 2 table. Keep the filenames, schemas, numeric precision, and regional mappings unchanged unless the scientific model is deliberately revised.
