# ====================================================================
# GreeksView Congressional Trading Crawler — Multi-Stage Production Image
# ====================================================================

# Stage 1: Build virtual environment with uv
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

# Install dependencies using frozen lockfile
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project

# Copy application source code and install project
COPY crawler ./crawler
RUN uv sync --frozen --no-dev

# ====================================================================
# Stage 2: Minimal hardened runtime image
# ====================================================================
FROM python:3.12-slim-bookworm AS runtime

WORKDIR /app

# Install minimal OS dependencies for PDF parsing, OCR, and healthcheck
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    tesseract-ocr \
    && rm -rf /var/lib/apt/lists/*

# Create dedicated non-root application user
RUN groupadd -g 10001 crawler && \
    useradd -u 10001 -g crawler -s /bin/bash -m crawler

# Copy prebuilt virtual environment and application code from builder
COPY --from=builder --chown=crawler:crawler /app/.venv /app/.venv
COPY --chown=crawler:crawler crawler /app/crawler
COPY --chown=crawler:crawler pyproject.toml README.md /app/

# Configure runtime PATH and environment
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDOONTWRITEBYTECODE=1 \
    PORT=8080

EXPOSE 8080

# Healthcheck testing the internal /healthz endpoint
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8080/healthz || exit 1

USER crawler

ENTRYPOINT ["crawler", "daemon"]
CMD ["--host", "0.0.0.0", "--port", "8080", "--initial-sync"]
