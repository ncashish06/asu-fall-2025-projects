#!/usr/bin/env bash
set -euo pipefail

# Database connection variables
export DB="${DB:-postgres}"
export USER="${USER:-postgres}"
export PASS="${PASS:-postgres}"
export HOST="${HOST:-127.0.0.1}"
export PORT="${PORT:-5432}"

cat > authors.ctl <<EOF
INPUT = authors.csv
OUTPUT = public.authors
TYPE = CSV
DELIMITER = ","
QUOTE = "\""
NULL = ""
SKIP = 1
EOF

cat > subreddits.ctl <<EOF
INPUT = subreddits.csv
OUTPUT = public.subreddits
TYPE = CSV
DELIMITER = ","
QUOTE = "\""
NULL = ""
SKIP = 1
EOF

cat > submissions.ctl <<EOF
INPUT = submissions.csv
OUTPUT = public.submissions
TYPE = CSV
DELIMITER = ","
QUOTE = "\""
NULL = ""
SKIP = 1
WRITER = DIRECT
EOF

cat > comments.ctl <<EOF
INPUT = comments.csv
OUTPUT = public.comments
TYPE = CSV
DELIMITER = ","
QUOTE = "\""
NULL = ""
SKIP = 1
WRITER = DIRECT
EOF

echo "Creating base tables"
psql -d "$DB" -U "$USER" -h "$HOST" -p "$PORT" -v ON_ERROR_STOP=1 -f create_tables.sql

echo "Loading authors.csv into authors table"
pg_bulkload  -d "$DB" -U "$USER" -h "$HOST" -p "$PORT" authors.ctl

echo "Loading subreddits.csv into subreddits table"
pg_bulkload  -d "$DB" -U "$USER" -h "$HOST" -p "$PORT" subreddits.ctl

echo "Loading submissions.csv into submissions table"
pg_bulkload  -d "$DB" -U "$USER" -h "$HOST" -p "$PORT" submissions.ctl

echo "Loading comments.csv into comments table"
pg_bulkload  -d "$DB" -U "$USER" -h "$HOST" -p "$PORT" comments.ctl

echo "Adding relations (PKs, FKs, Indexes)"
psql -d "$DB" -U "$USER" -h "$HOST" -p "$PORT" -v ON_ERROR_STOP=1 -f create_relations.sql

echo "Running ANALYZE to refresh query planner stats"
psql -d "$DB" -U "$USER" -h "$HOST" -p "$PORT" -v ON_ERROR_STOP=1 -c "ANALYZE;"

echo "Executing queries"
psql -d "$DB" -U "$USER" -h "$HOST" -p "$PORT" -v ON_ERROR_STOP=1 -f queries.sql

echo "All steps completed successfully"
