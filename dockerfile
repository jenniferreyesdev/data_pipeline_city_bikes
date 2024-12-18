FROM python:3.9-slim

WORKDIR /app

# Run system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy & install requirements.txt dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Make DBT directory for profiles
RUN mkdir -p ~/.dbt

# Copy all files - see .dockerignore for excluded files
COPY . .

# Environment variables
ENV PYTHONPATH=/app
ENV DBT_PROFILES_DIR=~/.dbt
ENV DBT_PROJECT_DIR=/app/citybikes_dbt

# Entrypoint.sh
RUN chmod +x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]