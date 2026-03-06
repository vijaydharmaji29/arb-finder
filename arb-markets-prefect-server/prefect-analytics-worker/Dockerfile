FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1

# System packages
RUN apt-get update && apt-get install -y \
    python3 \
    python3-venv \
    python3-pip \
    build-essential \
    curl \
    ca-certificates \
    git \
    && rm -rf /var/lib/apt/lists/*

# Create virtualenv
RUN python3 -m venv /opt/prefect
ENV PATH="/opt/prefect/bin:$PATH"

# --- GitHub auth for build phase ---
ARG GITHUB_TOKEN
RUN git config --global url."https://${GITHUB_TOKEN}@github.com/".insteadOf "https://github.com/"

# Copy and install requirements
COPY prefect-analytics-worker/requirements.txt /tmp/requirements.txt
RUN pip install --upgrade pip \
    && pip install -r /tmp/requirements.txt

# Default work pool can be overridden
ENV PREFECT_WORK_POOL=arb-analytics-prefect-pool

# Start worker (using process type - runs flows directly in this container)
CMD ["sh", "-c", "prefect worker start --pool ${PREFECT_WORK_POOL} --type process"]
