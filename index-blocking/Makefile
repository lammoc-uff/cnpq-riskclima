UV ?= uv

.PHONY: install climatology-era5 series-era5 climatology-cmip6 series-cmip6

install:
	$(UV) sync --frozen

climatology-era5:
	$(UV) run --frozen riskclima-blocking-climatology-era5

series-era5:
	$(UV) run --frozen riskclima-blocking-series-era5

climatology-cmip6:
	$(UV) run --frozen riskclima-blocking-climatology-cmip6

series-cmip6:
	$(UV) run --frozen riskclima-blocking-series-cmip6
