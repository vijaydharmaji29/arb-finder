# Use Python 3.12.1 as base image
FROM python:3.12.1-slim

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=5000

# Install system dependencies for psycopg2
RUN apt-get update && apt-get install -y \
    postgresql-client \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose port (default 5000, can be overridden via PORT env var)
EXPOSE 5000

# Run gunicorn with the config file
CMD ["gunicorn", "-c", "gunicorn_config.py", "wsgi:application"]

