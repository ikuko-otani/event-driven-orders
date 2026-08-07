FROM python:3.12-slim
RUN pip install --no-cache-dir uv==0.11.9
WORKDIR /app
COPY . .
RUN uv sync --frozen --package order-api --no-dev
EXPOSE 8000
CMD ["uv", "run", "--no-sync", "--package", "order-api", "uvicorn", "order_api.main:app", "--host", "0.0.0.0", "--port", "8000"]
