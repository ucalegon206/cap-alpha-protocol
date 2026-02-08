FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements FIRST to leverage Docker cache
COPY requirements-frozen.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements-frozen.txt

# Copy source code
COPY . .

# Create non-root user for security
RUN useradd -m appuser && chown -R appuser /app
USER appuser

# Default command
CMD ["python3", "src/run_trade_sim.py"]
