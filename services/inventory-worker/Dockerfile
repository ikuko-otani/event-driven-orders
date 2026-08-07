FROM python:3.12-slim
RUN pip install --no-cache-dir uv==0.11.9
WORKDIR /app
COPY . .
RUN uv sync --frozen --package inventory-worker --no-dev
CMD ["uv", "run", "--no-sync", "--package", "inventory-worker", "python", "-m", "inventory_worker.main"]
