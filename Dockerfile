FROM ghcr.io/astral-sh/uv:alpine
LABEL authors="techttgames"

WORKDIR /app

RUN adduser -S -u 1001 appuser && chown appuser .

COPY --chown=appuser pyproject.toml uv.lock ./

USER appuser

RUN uv sync --frozen --no-dev --no-install-project --python 3.14

COPY --chown=appuser hetman /app/hetman/
COPY --chown=appuser alembic.ini /app/alembic.ini
COPY --chown=appuser alembic/ /app/alembic/
COPY --chown=appuser README.md /app/

RUN uv sync --frozen --no-dev

CMD ["uv", "run", "python", "-m", "kumo_bot"]