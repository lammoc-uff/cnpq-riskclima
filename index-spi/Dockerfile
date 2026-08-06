# syntax=docker/dockerfile:1.7
ARG UV_VERSION=0.11.9

FROM ghcr.io/astral-sh/uv:${UV_VERSION} AS uv

FROM python:3.12-slim-bookworm AS runtime-env
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
    uv sync --frozen --no-dev --no-install-project
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable

FROM runtime-env AS runtime
ARG APP_UID=10001
RUN useradd --create-home --uid ${APP_UID} app && chown -R app:app /app
USER app
CMD ["riskclima-spi-cmip6"]

FROM runtime-env AS test
COPY tests ./tests
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-editable && \
    ruff check src tests && \
    pyrefly check && \
    pytest

FROM runtime AS default
