#!/bin/bash
# Create the roles and databases this stack needs.
#
# Runs once, on first initialisation of an empty PostgreSQL data directory
# (the official image executes /docker-entrypoint-initdb.d/* in order). It does
# not run again on restart, so it is not a migration mechanism - to re-run it,
# remove the volume: docker compose down -v
#
# Passwords come from the environment. They are passed to psql as variables
# rather than interpolated into the SQL text, so they are not expanded by the
# shell into a command line visible in the process list.

set -euo pipefail

: "${ETL_PASSWORD:?ETL_PASSWORD must be set}"
: "${AIRFLOW_DB_PASSWORD:?AIRFLOW_DB_PASSWORD must be set}"

psql -v ON_ERROR_STOP=1 \
     -v etl_password="${ETL_PASSWORD}" \
     -v airflow_password="${AIRFLOW_DB_PASSWORD}" \
     --username "${POSTGRES_USER}" --dbname postgres <<-'SQL'
    -- Pipeline role. NOSUPERUSER/NOCREATEROLE keeps it to what it needs:
    -- owning and writing its own databases.
    CREATE ROLE etl WITH LOGIN PASSWORD :'etl_password' NOSUPERUSER NOCREATEDB NOCREATEROLE;

    -- Airflow's metadata role, separate from the pipeline's.
    CREATE ROLE airflow WITH LOGIN PASSWORD :'airflow_password' NOSUPERUSER NOCREATEDB NOCREATEROLE;

    CREATE DATABASE airflow OWNER airflow;
    CREATE DATABASE congress_etl OWNER etl;
    CREATE DATABASE congress_etl_test OWNER etl;
SQL

# Revoke the default PUBLIC connect privilege so only the owner (and
# superusers) can reach each database.
for db in airflow congress_etl congress_etl_test; do
    psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname postgres \
         -c "REVOKE CONNECT ON DATABASE ${db} FROM PUBLIC;"
done

echo "init-databases.sh: created roles etl, airflow and databases airflow, congress_etl, congress_etl_test"
