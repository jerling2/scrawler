#!/bin/bash
set -e

set -a
. .env
set +a

echo "🔴 [postgres] Create database '$SCRAWLER_POSTGRES_DATABASE' ..."
docker exec -i scrawler-postgres psql -U "postgres" -d "postgres" -v scrawler_db="$SCRAWLER_POSTGRES_DATABASE" <<-EOSQL >/dev/null 
    \set ON_ERROR_STOP on
    CREATE DATABASE :scrawler_db;
EOSQL
echo "🟢 Done!"