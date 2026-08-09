UV ?= uv

.PHONY: install check run_spi_cmip6 run_spi_era5

install:
	$(UV) sync --frozen

check:
	$(UV) run --frozen ruff format --check src tests cmip6/scripts era5/scripts
	$(UV) run --frozen ruff check src tests cmip6/scripts era5/scripts
	$(UV) run --frozen pyrefly check
	$(UV) run --frozen pytest --cov --cov-report=term-missing

run_spi_cmip6:
	$(UV) run --frozen riskclima-spi-cmip6

run_spi_era5:
	$(UV) run --frozen riskclima-spi-era5
