UV ?= uv

.PHONY: install install-cdo climatology-era5 series-era5 climatology-cmip6 series-cmip6

install:
	$(UV) sync --frozen

install-cdo:
	@command -v apt-get >/dev/null 2>&1 || { echo "install-cdo requires Ubuntu or Debian with apt-get."; exit 1; }
	sudo apt-get update
	sudo apt-get install -y cdo
	cdo --version

climatology-era5:
	$(UV) run --frozen riskclima-blocking-climatology-era5

series-era5:
	$(UV) run --frozen riskclima-blocking-series-era5

climatology-cmip6:
	$(UV) run --frozen riskclima-blocking-climatology-cmip6

series-cmip6:
	$(UV) run --frozen riskclima-blocking-series-cmip6
