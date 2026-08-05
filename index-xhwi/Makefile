UV ?= uv
EXTRA ?= cpu
ARGS ?=
IMAGE ?= riskclima-xhwi
TAG ?= local
APPTAINER ?= apptainer
APPTAINER_IMAGE ?= riskclima-xhwi-cpu.sif
APPTAINER_SOURCE ?= docker-daemon://$(IMAGE):$(TAG)-cpu
PYTHON_PATHS := src tests era5/scripts era5land/scripts cmip6/scripts

.DEFAULT_GOAL := check

.PHONY: sync install format lint typecheck test check \
	era5-make-calibration era5-run-months era5-concat-months era5-run-all \
	era5land-make-calibration era5land-run-months era5land-concat-months era5land-run-all \
	cmip6-make-calibration cmip6-run-months cmip6-concat-months cmip6-run-all \
	era5-calibration era5-months era5-concat era5-all \
	era5land-calibration era5land-months era5land-concat era5land-all \
	cmip6-calibration cmip6-months cmip6-concat cmip6-all \
	docker-cpu docker-gpu docker-test apptainer-build

sync:
	$(UV) sync --frozen --extra $(EXTRA)

install: sync

format:
	$(UV) run --frozen --extra $(EXTRA) ruff format $(PYTHON_PATHS)
	$(UV) run --frozen --extra $(EXTRA) ruff check --fix $(PYTHON_PATHS)

lint:
	$(UV) run --frozen --extra $(EXTRA) ruff check $(PYTHON_PATHS)

typecheck:
	$(UV) run --frozen --extra $(EXTRA) pyrefly check

test:
	$(UV) run --frozen --extra $(EXTRA) pytest $(ARGS)

check: lint typecheck test

era5-make-calibration:
	$(UV) run --frozen --extra $(EXTRA) riskclima-xhwi-era5-make-calibration $(ARGS)

era5-run-months:
	$(UV) run --frozen --extra $(EXTRA) riskclima-xhwi-era5-run-months $(ARGS)

era5-concat-months:
	$(UV) run --frozen --extra $(EXTRA) riskclima-xhwi-era5-concat-months $(ARGS)

era5-run-all:
	$(UV) run --frozen --extra $(EXTRA) riskclima-xhwi-era5-run-all $(ARGS)

era5land-make-calibration:
	$(UV) run --frozen --extra $(EXTRA) riskclima-xhwi-era5land-make-calibration $(ARGS)

era5land-run-months:
	$(UV) run --frozen --extra $(EXTRA) riskclima-xhwi-era5land-run-months $(ARGS)

era5land-concat-months:
	$(UV) run --frozen --extra $(EXTRA) riskclima-xhwi-era5land-concat-months $(ARGS)

era5land-run-all:
	$(UV) run --frozen --extra $(EXTRA) riskclima-xhwi-era5land-run-all $(ARGS)

cmip6-make-calibration:
	$(UV) run --frozen --extra $(EXTRA) riskclima-xhwi-cmip6-make-calibration $(ARGS)

cmip6-run-months:
	$(UV) run --frozen --extra $(EXTRA) riskclima-xhwi-cmip6-run-months $(ARGS)

cmip6-concat-months:
	$(UV) run --frozen --extra $(EXTRA) riskclima-xhwi-cmip6-concat-months $(ARGS)

cmip6-run-all:
	$(UV) run --frozen --extra $(EXTRA) riskclima-xhwi-cmip6-run-all $(ARGS)

# Short aliases used in the operational documentation.
era5-calibration: era5-make-calibration
era5-months: era5-run-months
era5-concat: era5-concat-months
era5-all: era5-run-all

era5land-calibration: era5land-make-calibration
era5land-months: era5land-run-months
era5land-concat: era5land-concat-months
era5land-all: era5land-run-all

cmip6-calibration: cmip6-make-calibration
cmip6-months: cmip6-run-months
cmip6-concat: cmip6-concat-months
cmip6-all: cmip6-run-all

docker-cpu:
	docker build --target cpu -t $(IMAGE):$(TAG)-cpu .

docker-gpu:
	docker build --target gpu -t $(IMAGE):$(TAG)-gpu .

docker-test:
	docker build --target test -t $(IMAGE):$(TAG)-test .

apptainer-build: docker-cpu
	$(APPTAINER) build $(ARGS) $(APPTAINER_IMAGE) $(APPTAINER_SOURCE)
