FROM ghcr.io/astral-sh/uv:0.11.9 AS uv

FROM python:3.12-slim AS base
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy
WORKDIR /app
COPY --from=uv /uv /uvx /bin/
COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --frozen --no-dev
RUN useradd --create-home --uid 10001 appuser && chown -R appuser:appuser /app

FROM base AS runtime
COPY --chown=appuser:appuser scripts ./scripts
COPY --chown=appuser:appuser .env.example ./
USER appuser
ENTRYPOINT ["/app/.venv/bin/python", "scripts/run_download.py"]
CMD ["--help"]

FROM base AS test
RUN uv sync --frozen
COPY scripts ./scripts
COPY tests ./tests
COPY .env.example ./
RUN uv run --frozen pytest
