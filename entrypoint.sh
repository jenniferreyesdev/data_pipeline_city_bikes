#!/bin/bash
set -e

# Get Postgres connection details from environment variables
PG_HOST="${DESTINATION__POSTGRES__CREDENTIALS__HOST}"
PG_PORT="${DESTINATION__POSTGRES__CREDENTIALS__PORT:-5432}"
PG_USER="${DESTINATION__POSTGRES__CREDENTIALS__USERNAME}"
PG_PASSWORD="${DESTINATION__POSTGRES__CREDENTIALS__PASSWORD}"
PG_DB="${DESTINATION__POSTGRES__CREDENTIALS__DATABASE}"

# Function to check if postgres is ready
postgres_ready() {
    PGPASSWORD=$PG_PASSWORD psql -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" -p "$PG_PORT" -c "SELECT 1" >/dev/null 2>&1
}

# Wait for postgres to become ready
echo "Waiting for PostgreSQL to become ready..."
until postgres_ready; 
do
  echo "PostgreSQL is unavailable - sleeping"
  sleep 2
done

echo "PostgreSQL is ready! Starting citybikes pipeline..."
python citybikes-pipeline.py