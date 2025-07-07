# Dockerfile
FROM apache/airflow:2.5.1

USER root

# Install system build tools required for duckdb
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    git \
    curl \
    libssl-dev \
    libffi-dev \
    python3-dev \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Upgrade pip and install Python packages
RUN pip install --upgrade pip \
 && pip install pandas textblob duckdb
