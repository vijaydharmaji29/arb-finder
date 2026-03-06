FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1

# System packages
RUN apt-get update && apt-get install -y \
    python3 \
    python3-venv \
    python3-pip \
    build-essential \
    libpq-dev \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create virtualenv
RUN python3 -m venv /opt/prefect
ENV PATH="/opt/prefect/bin:$PATH"

# Install Prefect with async PostgreSQL driver
RUN pip install --upgrade pip \
    && pip install prefect asyncpg sqlalchemy requests

# Expose Prefect UI and API
EXPOSE 4200

# Start Prefect Server
CMD ["prefect", "server", "start", "--host", "0.0.0.0"]
