#!/usr/bin/env bash
set -euo pipefail

# Database connection variables
export DB="${DB:-postgres}"
export USER="${USER:-postgres}"
export PASS="${PASS:-postgres}"
export HOST="${HOST:-127.0.0.1}"
export PORT="${PORT:-5432}"

echo "Creating base tables"
psql -d "$DB" -U "$USER" -h "$HOST" -p "$PORT" -v ON_ERROR_STOP=1 -f create_tables.sql

echo "Loading authors.csv into authors table"
psql -d "$DB" -U "$USER" -h "$HOST" -p "$PORT" -v ON_ERROR_STOP=1 \
  -c "\copy authors FROM 'authors.csv' CSV HEADER NULL ''"

echo "Loading subreddits.csv into subreddits table"
psql -d "$DB" -U "$USER" -h "$HOST" -p "$PORT" -v ON_ERROR_STOP=1 \
  -c "\copy subreddits FROM 'subreddits.csv' CSV HEADER NULL ''"

echo "Loading submissions.csv into submissions table"
psql -d "$DB" -U "$USER" -h "$HOST" -p "$PORT" -v ON_ERROR_STOP=1 \
  -c "\copy submissions FROM 'submissions.csv' CSV HEADER NULL ''"

echo "Loading comments.csv into comments table"
psql -d "$DB" -U "$USER" -h "$HOST" -p "$PORT" -v ON_ERROR_STOP=1 \
  -c "\copy comments FROM 'comments.csv' CSV HEADER NULL ''"

echo "Adding relations (PKs, FKs, Indexes)"
psql -d "$DB" -U "$USER" -h "$HOST" -p "$PORT" -v ON_ERROR_STOP=1 -f create_relations.sql

echo "Running ANALYZE to refresh query planner stats"
psql -d "$DB" -U "$USER" -h "$HOST" -p "$PORT" -v ON_ERROR_STOP=1 -c "ANALYZE;"

echo "Executing queries"
psql -d "$DB" -U "$USER" -h "$HOST" -p "$PORT" -v ON_ERROR_STOP=1 -f queries.sql

echo "All steps completed successfully"
