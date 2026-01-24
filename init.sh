#!/usr/bin/env bash
set -euo pipefail

ENV_FILE=".env"

# === Step 1: Generate .env if not exists ===
# Copies .env.example and generates a random admin secret.
# The secret is a random 32-byte hex string generated via openssl.
if [ ! -f "$ENV_FILE" ]; then
  echo "[init] Creating $ENV_FILE from .env.example..."
  cp .env.example "$ENV_FILE"
  ADMIN_SECRET=$(openssl rand -hex 32)
  sed -i "s/^HASURA_GRAPHQL_ADMIN_SECRET=.*/HASURA_GRAPHQL_ADMIN_SECRET=$ADMIN_SECRET/" "$ENV_FILE"
  echo "[init] Generated random admin secret in $ENV_FILE"
else
  echo "[init] Using existing $ENV_FILE"
fi

# === Step 2: Verify admin secret exists ===
# Safety check: ensure .env actually contains HASURA_GRAPHQL_ADMIN_SECRET.
if ! grep -q "HASURA_GRAPHQL_ADMIN_SECRET=" "$ENV_FILE"; then
  echo "[init] ERROR: HASURA_GRAPHQL_ADMIN_SECRET missing in $ENV_FILE"
  exit 1
fi

# === Step 3: Rebuild services ===
# Rebuild all Docker images with no cache to ensure Go code + dependencies
# are freshly compiled (avoids stale layers).
echo "[init] Building Docker images (no cache)..."
docker compose --progress=plain build --no-cache

# === Step 4: Start services ===
# Starts all containers in the background.
echo "[init] Starting services with Docker Compose..."
docker compose up -d

# === Step 5: Wait for Hasura to come up ===
# Polls Hasura's /healthz endpoint until it returns 200 OK (max 30 tries).
# Read port from .env (default 8081)
HASURA_PORT=$(grep -E "^HASURA_PORT=" "$ENV_FILE" | cut -d'=' -f2 || echo "8081")
HASURA_PORT=${HASURA_PORT:-8081}

echo "[init] Waiting for Hasura to start on port $HASURA_PORT..."
TRIES=0
until curl -s "http://localhost:${HASURA_PORT}/healthz" >/dev/null; do
  TRIES=$((TRIES+1))
  if [ $TRIES -gt 30 ]; then
    echo "[init] ERROR: Hasura did not start in time."
    exit 1
  fi
  sleep 2
done

echo "[init] ✅ Hasura is running at http://localhost:${HASURA_PORT}/console"
echo "[init] ✅ Health check via GraphQL: query { indexer_health { latest_block_height } }"
echo "[init] Admin secret stored in .env"
