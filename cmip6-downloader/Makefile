.PHONY: install sync format lint typecheck test check compare download all docker-build docker-test apptainer-build

ARGS ?=
IMAGE ?= cmip6-downloader:latest
SIF ?= cmip6-downloader.sif

install:
	uv sync --frozen

sync:
	uv sync --frozen

format:
	uv run --frozen ruff format scripts tests
	uv run --frozen ruff check --fix scripts tests

lint:
	uv run --frozen ruff format --check scripts tests
	uv run --frozen ruff check scripts tests

typecheck:
	uv run --frozen pyrefly check

test:
	uv run --frozen pytest --cov --cov-report=term-missing

check: lint typecheck test

compare:
	uv run --frozen python scripts/compare_catalogs.py $(ARGS)

download:
	uv run --frozen python scripts/run_download.py $(ARGS)

all: compare download

docker-build:
	docker build --target runtime -t $(IMAGE) .

docker-test:
	docker build --target test .

apptainer-build: docker-build
	apptainer build $(SIF) docker-daemon://$(IMAGE)
