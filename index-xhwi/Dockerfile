# syntax=docker/dockerfile:1.7
ARG UV_VERSION=0.11.9

FROM ghcr.io/astral-sh/uv:${UV_VERSION} AS uv

FROM python:3.12-slim-bookworm AS cpu-env
COPY --from=uv /uv /uvx /bin/
ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project --extra cpu
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable --extra cpu

FROM cpu-env AS cpu
ARG APP_UID=10001
RUN useradd --create-home --uid ${APP_UID} app && chown app:app /app
USER app
ENTRYPOINT ["riskclima-xhwi-era5-run-all"]
CMD ["--help"]

FROM cpu-env AS test
COPY tests ./tests
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-editable --extra cpu && \
    ruff check src tests && \
    pyrefly check && \
    pytest

FROM nvidia/cuda:12.8.1-runtime-ubuntu24.04 AS gpu-env
COPY --from=uv /uv /uvx /bin/
ENV DEBIAN_FRONTEND=noninteractive \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
RUN apt-get update && \
    apt-get install --no-install-recommends -y python3.12 ca-certificates && \
    rm -rf /var/lib/apt/lists/* && \
    ln -s /usr/bin/python3.12 /usr/local/bin/python
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project --extra gpu
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable --extra gpu

FROM gpu-env AS gpu
ARG APP_UID=10001
RUN useradd --create-home --uid ${APP_UID} app && chown app:app /app
USER app
ENTRYPOINT ["riskclima-xhwi-era5-run-all"]
CMD ["--help"]

FROM cpu AS default
