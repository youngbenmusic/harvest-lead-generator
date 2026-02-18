FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY tools/ tools/
COPY migrations/ migrations/
COPY workflows/ workflows/
COPY data/ data/

# Create temp directory
RUN mkdir -p .tmp

# Default command: run the full pipeline
CMD ["python", "tools/orchestrator.py"]
