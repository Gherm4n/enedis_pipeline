FROM python:3.12-slim

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Copy dependency files
COPY pyproject.toml uv.lock ./
# No lockfile yet, so we sync based on pyproject.toml
RUN uv sync --frozen --no-install-project

# Copy source code
COPY src/ ./src/

# Expose Dash port
EXPOSE 8050

# Run the app
CMD ["uv", "run", "src/app.py"]
