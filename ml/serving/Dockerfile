FROM python:3.11-slim

WORKDIR /app

# Security: run as non-root user
RUN groupadd -r appuser && useradd -r -g appuser appuser

COPY pyproject.toml ./
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir duckdb==0.10.3 fastapi==0.111.0 \
    uvicorn[standard]==0.30.1 pydantic==2.7.1 pydantic-settings==2.3.1 \
    structlog==24.2.0 python-dotenv==1.0.1

COPY ml/serving/ ./ml/serving/

RUN chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

CMD ["uvicorn", "ml.serving.app:app", "--host", "0.0.0.0", "--port", "8000"]
