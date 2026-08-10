UV ?= uv

.PHONY: install process-era5 index-era5 process-cmip6 index-cmip6

install:
	$(UV) sync --frozen

process-era5:
	$(UV) run --frozen riskclima-sacz-process-era5 $(ARGS)

index-era5:
	$(UV) run --frozen riskclima-sacz-index-era5 $(ARGS)

process-cmip6:
	$(UV) run --frozen riskclima-sacz-process-cmip6 $(ARGS)

index-cmip6:
	$(UV) run --frozen riskclima-sacz-index-cmip6 $(ARGS)
