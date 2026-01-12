# Dockerfile for SAPN Prometheus Scraper
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements first for better layer caching
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ ./src/

# Expose Prometheus metrics port
EXPOSE 9120

# Run the application
CMD ["python", "-m", "src.main"]
