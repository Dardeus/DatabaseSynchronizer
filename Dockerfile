FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq5 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY sync_db.py .
RUN chmod +x sync_db.py

RUN useradd --create-home --shell /bin/bash appuser
USER appuser

ENTRYPOINT ["python", "sync_db.py"]
CMD ["--help"]