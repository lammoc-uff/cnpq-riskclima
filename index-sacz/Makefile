UV ?= uv

.PHONY: install process-era5 index-era5 consolidate-era5 monthly-era5 \
	process-cmip6 index-cmip6 consolidate-cmip6 monthly-cmip6

install:
	$(UV) sync --frozen

process-era5:
	$(UV) run --frozen riskclima-sacz-process-era5 $(ARGS)

index-era5:
	$(UV) run --frozen riskclima-sacz-index-era5 $(ARGS)

consolidate-era5:
	$(UV) run --frozen riskclima-sacz-consolidate-era5

monthly-era5:
	$(UV) run --frozen riskclima-sacz-monthly-era5

process-cmip6:
	$(UV) run --frozen riskclima-sacz-process-cmip6 $(ARGS)

index-cmip6:
	$(UV) run --frozen riskclima-sacz-index-cmip6 $(ARGS)

consolidate-cmip6:
	$(UV) run --frozen riskclima-sacz-consolidate-cmip6

monthly-cmip6:
	$(UV) run --frozen riskclima-sacz-monthly-cmip6
