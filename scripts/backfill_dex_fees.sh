#!/usr/bin/env bash
# =============================================================================
# Convenience wrapper for scripts/backfill_dex_fees.sql
# =============================================================================
# Runs the dex-fee backfill against a running indexer Postgres container, fixing
# dex_pool_fee_events rows that were stored empty before the indexer learned the
# 8-token pendulum fee layout. See backfill_dex_fees.sql for the full rationale.
#
# Usage:
#   scripts/backfill_dex_fees.sh [PG_CONTAINER]
#
#   PG_CONTAINER   docker container running Postgres
#                  (default: magi-mongo-indexer-postgres-1; the testnet stack is
#                   magi-mongo-indexer-debug-testnet-postgres-1)
#
# Env overrides:
#   PGUSER      (default: indexer)
#   PGDATABASE  (default: indexerdb)
#   PGPASSWORD  (default: indexerpass)
#
# To run against a Postgres reachable by DSN instead of via docker exec:
#   psql "<DSN>" -v ON_ERROR_STOP=1 -f scripts/backfill_dex_fees.sql
# =============================================================================
set -euo pipefail

CONTAINER="${1:-magi-mongo-indexer-postgres-1}"
PGUSER="${PGUSER:-indexer}"
PGDATABASE="${PGDATABASE:-indexerdb}"
PGPASSWORD="${PGPASSWORD:-indexerpass}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if ! docker inspect "$CONTAINER" >/dev/null 2>&1; then
  echo "error: container '$CONTAINER' not found. Pass the Postgres container name as arg 1." >&2
  echo "  running candidates:" >&2
  docker ps --format '  {{.Names}}' | grep -i postgres >&2 || true
  exit 1
fi

echo "Backfilling dex_pool_fee_events  (container=$CONTAINER db=$PGDATABASE)…"
docker exec -i -e PGPASSWORD="$PGPASSWORD" "$CONTAINER" \
  psql -U "$PGUSER" -d "$PGDATABASE" -v ON_ERROR_STOP=1 \
  < "$SCRIPT_DIR/backfill_dex_fees.sql"
echo "Done."
