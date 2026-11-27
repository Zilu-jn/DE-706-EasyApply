# Reproducible development environment for the EasyApply pipeline.
# Same tooling as local development: uv manages the exact locked dependencies.
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1

# Install locked dependencies first so code changes don't invalidate this layer.
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen

COPY . .

# Default: run the offline test suite. Override with
#   docker compose run --rm app uv run python -m ingestion.run
CMD ["uv", "run", "pytest", "-q"]
