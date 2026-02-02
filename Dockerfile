FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    unzip \
    curl \
    git \
    make \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Chromium (compatible with ARM64)
RUN apt-get update && apt-get install -y \
    chromium \
    chromium-driver \
    && rm -rf /var/lib/apt/lists/*

# Environment variables for the scraper to locate the binaries
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_BIN=/usr/bin/chromium-driver

# Set up workspace
WORKDIR /app

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install webdriver-manager

# Copy project
COPY . .

# Default command (overridden by docker-compose)
CMD ["python", "src/run_historical_scrape.py", "--help"]
