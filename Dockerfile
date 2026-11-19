FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS builder
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=0
WORKDIR /app

# 1) Dependencies only — cached until pyproject.toml or uv.lock change
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-dev

# 2) Project source
COPY pyproject.toml uv.lock README.md ./
COPY src/ ./src/
RUN --mount=type=cache,target=/root/.cache/uv uv sync --locked --no-dev --no-editable

FROM python:3.13-slim-bookworm
RUN useradd --create-home app
WORKDIR /app
COPY --from=builder --chown=app:app /app/.venv /app/.venv
COPY --chown=app:app frontend/ ./frontend/
COPY --chown=app:app db/ ./db/
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1
USER app
