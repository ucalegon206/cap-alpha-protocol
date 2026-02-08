FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    make \
    chromium \
    chromium-driver \
    && rm -rf /var/lib/apt/lists/*

# Set Chrome environment variables
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_BIN=/usr/bin/chromium-driver
ENV PYTHONUNBUFFERED=1

# Copy requirements from pipeline dir
COPY pipeline/requirements-frozen.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements-frozen.txt

# Copy source code from pipeline dir
COPY pipeline/ .

# Copy contracts
COPY contracts /app/contracts

# Create non-root user for security
RUN useradd -m appuser && chown -R appuser /app
# Ensure volumes are writable by appuser
RUN mkdir -p /app/models /app/data && chown -R appuser /app/models /app/data

USER appuser

# Default command
CMD ["tail", "-f", "/dev/null"]
