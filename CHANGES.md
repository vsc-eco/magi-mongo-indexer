# Changes from go-vsc-indexer to magi-mongo-indexer

This document summarizes the changes made to convert go-vsc-indexer into magi-mongo-indexer.

## Overview

The magi-mongo-indexer is a modified version of go-vsc-indexer that **directly accesses MongoDB** instead of using GraphQL WebSocket subscriptions to fetch contract logs from the go-vsc-node.

## Key Changes

### 1. Data Source Change
- **Before**: Used GraphQL WebSocket subscriptions to receive real-time contract logs
- **After**: Polls MongoDB `contract_state` collection directly from the `go-vsc` database

### 2. New Files Created

#### `/internal/indexer/fetcher/mongo.go`
New MongoDB polling implementation that:
- Connects to MongoDB using the official Go MongoDB driver
- Polls the `contract_state` collection at configurable intervals
- Tracks last processed block height per contract
- Converts MongoDB documents to LogEvent format
- Inserts logs into PostgreSQL tables

### 3. Modified Files

#### `/go.mod`
- Changed module name from `github.com/tibfox/go-vsc-indexer` to `github.com/tibfox/magi-mongo-indexer`
- Removed `github.com/gorilla/websocket` (no longer needed)
- Added `go.mongodb.org/mongo-driver v1.17.1` (MongoDB driver)

#### `/internal/config/config.go`
- Removed WebSocket-related configuration (`WSURL`, `BackfillURL`)
- Added MongoDB configuration:
  - `MongoURI`: MongoDB connection string (default: `mongodb://go-vsc-node:27017`)
  - `MongoDBName`: Database name (default: `go-vsc`)
  - `PollInterval`: Polling interval in seconds (default: 5 seconds, configurable via `POLL_INTERVAL_SEC`)

#### `/cmd/indexer/main.go`
- Removed WebSocket subscription loop
- Added MongoDB polling loop with automatic reconnection
- Updated watcher callback (no longer needs to restart WebSocket connections)

#### `/docker-compose.yaml`
- Renamed service from `magi-indexer` to `magi-mongo-indexer`
- Removed GraphQL-related environment variables (`WS_URL`, `BACKFILL_URL`)
- Added MongoDB configuration variables:
  - `MONGO_URI`
  - `MONGO_DB_NAME`
  - `POLL_INTERVAL_SEC`

#### `/Readme.md`
- Updated title to "Magi MongoDB Indexer"
- Changed description to reflect MongoDB polling instead of GraphQL subscriptions
- Updated configuration examples

### 4. Removed Files
- `.git/` directory (removed git relations as requested)

### 5. Unchanged Components

The following components remain unchanged and work the same way:
- PostgreSQL database schema and tables
- Hasura GraphQL integration
- Mapping and view configuration files
- Log parsing and data transformation logic
- File watcher for config updates
- Backfill functionality (still uses `contract_logs` table)

## MongoDB Collection Structure

The indexer reads documents from the `contract_state` collection with this structure:

```javascript
{
  "_id": ObjectId("..."),
  "id": "bafyreiglyi7gjeyu7k3tw7ci2h4n7bhqgubtysg3wn7jzw65yfq3e22kgy",
  "block_height": NumberLong(96043960),
  "contract_id": "vsc1Bp8ykBKDT74vYrZShhfEhp8Mn8bG2ChiAf",
  "inputs": ["d1ddc45b72f9312c8a2172630813bf2f77535bb3"],
  "results": [
    {
      "ret": "success",
      "ok": true,
      "logs": ["log_type:param1=value1|param2=value2"]
    }
  ],
  "metadata": { "currentsize": 0, "maxsize": 0 },
  "state_merkle": "QmUNLLsPACCz1vLxQVkXqqLX5R1X345qqfHbsf67hvA3Nn"
}
```

**Key Points:**
- Logs are in the `results[].logs[]` array (nested structure)
- Contract identifier is `contract_id` (not `contract_address`)
- The indexer creates a pseudo `tx_hash` from the document ID and block height
- Only processes documents where `results.logs` exists and is not empty

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `MONGO_URI` | MongoDB connection URI | `mongodb://go-vsc-node:27017` |
| `MONGO_DB_NAME` | MongoDB database name | `go-vsc` |
| `POLL_INTERVAL_SEC` | Polling interval in seconds | `5` |
| `DATABASE_URL` | PostgreSQL connection string | (existing) |
| `HASURA_URL` | Hasura endpoint | (existing) |
| `HASURA_ADMIN_SECRET` | Hasura admin secret | (existing) |
| `MAPPINGS_FILES` | Path to mapping files | (existing) |
| `VIEWS_FILES` | Path to view files | (existing) |

## How It Works

1. **Initialization**: On startup, the indexer loads contract mappings and determines the last processed block height for each contract from the PostgreSQL database.

2. **Polling Loop**: Every `POLL_INTERVAL_SEC` seconds, the indexer:
   - Queries MongoDB for new entries in `contract_state` where `block_height` > last processed height
   - Processes entries in order of block height
   - Inserts raw logs into PostgreSQL `contract_logs` table
   - Parses and inserts structured data into contract-specific tables
   - Updates the last processed block height

3. **Error Handling**: If MongoDB connection fails, the indexer automatically retries with exponential backoff.

4. **Config Watching**: The file watcher continues to monitor mapping and view files for changes, automatically updating tables and Hasura metadata without requiring a restart.

## Benefits of MongoDB Access

1. **Simpler Architecture**: No need for WebSocket connection management
2. **Resilient**: Can catch up on missed logs after downtime
3. **Direct Access**: Queries MongoDB directly without intermediate GraphQL layer
4. **Flexible**: Easy to adjust polling interval based on load requirements
5. **Debuggable**: Can manually query MongoDB to verify data before indexing

## Migration Notes

If you're migrating from go-vsc-indexer to magi-mongo-indexer:

1. Your existing PostgreSQL tables, mappings, and views remain compatible
2. The indexer will automatically start from the last indexed block height
3. No data migration is needed
4. Update your `docker-compose.yaml` to use MongoDB connection instead of WebSocket URL
