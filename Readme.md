# Magi MongoDB Indexer

A lightweight MongoDB-based indexer for [`go-vsc-node`](https://github.com/vsc-eco/go-vsc-node).
The **indexer** polls the MongoDB `contract_state` collection directly from the `go-vsc` database to fetch **contract logs**. Each event for defined contracts will get stored in a normalized form within **Postgres**. Then **Hasura** exposes these records via GraphQL for easy querying by dapps including GraphQL subscription support. The administrator can login to the hasura instance via http://localhost:8081/hasura/console by using a randomly created secret (see below). If Hasura is exposed to the internet (by default) everyone can run queries or add subscriptions. The indexer by default expects a running `go-vsc-node` (observer or validator node) with MongoDB accessible on the same network. This can be overridden in the `docker-compose.yaml`.

## Run with Docker Compose

First-time setup:

```bash
chmod +x init.sh
./init.sh
```

This will:

1. Generate a random admin secret in `.env` (only on first run).
2. Start Postgres, Hasura, and the indexer via Docker Compose.

Next time, you can simply use:

```bash
docker compose up -d
```

Services:

* **Postgres** → `localhost:5432` (configurable via `POSTGRES_PORT`)
* **Hasura GraphQL** → `http://localhost:8081/hasura/console` (configurable via `HASURA_PORT`)
* **Indexer** (Go ingestor) → polls MongoDB contract_state collection and writes logs into Postgres

### Running Multiple Instances

To run multiple indexers on the same system, copy the entire folder and customize the ports in `.env`:

```bash
# Instance 1 (.env)
POSTGRES_PORT=5432
HASURA_PORT=8081

# Instance 2 (.env)
POSTGRES_PORT=5433
HASURA_PORT=8082
```

Each instance needs its own Postgres volume, so also update the volume name in `docker-compose.yaml` or use separate project names:

```bash
docker compose -p indexer-instance2 up -d
```

### Health Check

The indexer creates an `indexer_health` view queryable through Hasura's GraphQL endpoint (no extra port needed):

```graphql
query {
  indexer_health {
    latest_block_height
    last_update
    tracked_contracts
    total_logs
  }
}
```

Response:
```json
{
  "data": {
    "indexer_health": [
      {
        "latest_block_height": 892341,
        "last_update": "2025-01-24T12:34:56",
        "tracked_contracts": 4,
        "total_logs": 15234
      }
    ]
  }
}
```

For basic container health, use Hasura's built-in endpoint: `curl http://localhost:8081/healthz`

If you need to change the MongoDB connection or polling interval, you can modify the `docker-compose.yaml`:
```
  magi-mongo-indexer:
    ...
    environment:
      ...
      MONGO_URI: mongodb://mongo_vsc:27017           # MongoDB connection URI
      MONGO_DB_NAME: go-vsc                          # Database name
      POLL_INTERVAL_SEC: 5                           # Polling interval in seconds
      ...
```

## Mappings

The indexer only cares for logs that fit to contract IDs described in the contract-specific mapping files under `internal/config/events/*_mappings.yaml`. Each file contains the schema for one contract (or a logical group) and defines how logs should be parsed and stored in Postgres.

**Important for contract developers:**
Logs that are emitted by `sdk.Log()` are only stored when the transaction succeeds. The indexer only supports `json` or `csv` formatted logs with a specified log type that is:
- csv: first token
- json: a field called "type" 

(examples see below)

## Log Mappings in `internal/config/events/*_mappings.yaml`

Each event entry defines how an incoming log should be parsed and stored in Postgres. If a mappings file gets extended with a new contract, the indexer will automatically fetch all historic logs from MongoDB starting at the specified `fromBlockHeight` and then continue polling for new logs.

### Parse `json`
#### Example Event json

```yaml
events:
  - contract: "vscContractId" # contract address
    log_type: "mint" # type of the log event
    table: "contractslug_mint_events" # target table name
    schema: # columns in the target table (numeric, string and bool)
      id: numeric
      by: string
      email: string
    parse: "json" # parse mode of the log
    fields: # jsonPath expressions (supports nested paths)
      id: "$.id"
      by: "$.by"
      email: "$.user.profile.email"  # nested paths supported
```
Additional to these mappings we also store columns based of the transaction the log is related to:
- indexer_block_height
- indexer_tx_hash
- indexer_ts
- indexer_id (running int)

#### Example log

```json
{ "type": "mint", "id": 123, "by": "bob", "user": { "profile": { "email": "bob@example.com" } } }
```

#### Stored in Postgres in table `contractslug_mint_events`

| indexer_block_height | indexer_tx_hash | indexer_ts          | id  | by  | email           |
| -------------------- | --------------- | ------------------- | --- | --- | --------------- |
| 12345                | 0xabc           | 2025-10-02T22:00:33 | 123 | bob | bob@example.com |


### Parse `csv`
#### Example Event

```yaml
events:
  - contract: "vscContractId" # contract address
    log_type: "mint" # type of the log event
    table: "contractslug_mint_events" # target table name
    schema: # columns in the target table (numeric, string and bool)
        id: numeric 
        by: string
    parse: "csv" # parse mode of the log
    delimiter: "|"         # split by pipe
    key_delimiter: "="     # parse key=value format
    fields: # remember: first token (index 0) is reserved for the log type
        id: "1"            # "id=42" → "42"
        by: "2"            # "by=bob" → "bob"
```
Additional to these mappings we also store the indexer_* columns.

#### Example log

```
mint|id=123|by=bob
```

 The example output will be the same as above.


## View Definitions in `internal/config/events/*_views.yaml`

Views let you define custom query layers over your indexed tables. They are automatically created in Postgres and exposed by Hasura. Views allow you to shape or aggregate raw log data into a form that is directly usable by dapps, monitoring tools, and other downstream services. Each file ending with `_views.yaml` can bundle related SQL views.

#### Example

```yaml
view:
  - name: "contractslug_mints_view"
    sql: |
      CREATE OR REPLACE VIEW contractslug_mints_view AS
      SELECT id, by, indexer_block_height AS bh
      FROM contractslug_mint_csv_events
```

#### Example output

| id  | by  | bh       |
| ----|---- | -------- |
| 123 | bob | 99949930 |


#### Query in GraphQL

Hasura is configured that everyone can access the GraphQL endpoint `http://localhost:8081/api/v1/graphql` run queries and add subscriptions (via `ws://`):

Run a query:
```graphql
query {
  okinoko_escrow_decision_events(order_by: {indexer_id: desc}, limit: 5) {
    id
    role
    address
    decision
    indexer_id
  }
}
```

Establish a subscription:
```graphql
{"id":"1","type":"subscribe","payload":{"query":"subscription { okinoko_escrow_decision_events_stream(batch_size: 10, cursor: { initial_value: { indexer_id: 0 }, ordering: ASC }) { id role address decision indexer_id } }"}}

```


Output:

```json
{
  "data": {
    "contractslug_mints_view": [
      {
        "bh": 12345,
        "id": 123,
        "by": "bob"
      }
    ]
  }
}
```



## 🔄 Updating Mappings / Views

The indexer includes a watcher that monitors every `*_mappings.yaml` and `*_views.yaml` file within the configured directories.

On change:
1. Files are reloaded
2. New tables/views are created
3. Old tables/views (not in config) are pruned
4. Hasura metadata is synced

➡️ No restart required.

**Limitations:**
- On-the-fly schema changes (renaming/changing column types) are not supported.
- To apply changes, either:
  - Use a new table/view name, or
  - Delete the old one, wait for the watcher to remove it, then re-add with the new schema.

## Query generic logs for debugging

Every raw log (fitting to mapped contracts) is also stored in the generic `contract_logs` table. Example query in Hasura:
```graphql
query {
  contract_logs(order_by: {block_height: desc}, limit: 5) {
    block_height
    tx_hash
    contract_address
    log
    ts
  }
}
```
This is useful for debugging new mappings or inspecting unexpected logs.

## Backfilling testnet DEX pools

The live indexer auto-discovers DEX pools by watching for `pool_init` logs emitted during contract initialization. Some testnet pools were deployed from older contract versions that did not emit this log, so they were never discovered. These pools need a one-time manual backfill.

The current DEX contracts (`dex-contracts/`) **do** emit all required logs (`pool_init`, `swap`, `fee`, `amt`, `add_liq`, `rem_liq`, `migrate`), so any newly deployed pool will be indexed automatically. The backfill is only needed for legacy pools.

### When to backfill

You need to backfill a pool if:
- The pool was deployed before logging was added to `Init`
- The pool was migrated and its `init` call didn't produce a `pool_init` log
- Swaps go through the router (`execute` action) and the pool isn't in `discovered_contracts`

You can check with:
```sql
-- Run inside the postgres container
SELECT contract_id FROM discovered_contracts WHERE discover_event = 'pool_init';
```

### How to backfill

There are two scripts in `scripts/`. Both are idempotent (safe to re-run).

#### Option 1: MongoDB direct (recommended, full history)

Reads all contract outputs directly from MongoDB. Handles all payload formats including BSON Binary from router `execute` calls.

```bash
npm install  # needs pg + mongodb packages
node scripts/backfill_dex_pool_mongo.js <POOL_CONTRACT_ID> [MONGO_URI] [PG_URL]
```

Example:
```bash
node scripts/backfill_dex_pool_mongo.js \
  vsc1BgwiEg8P5u2qYSV7DL8FCqrj5E7hWSYKmf \
  "mongodb://127.0.0.1:27022" \
  "postgres://indexer:indexerpass@127.0.0.1:5433/indexerdb"
```

This script:
1. Fetches all successful contract outputs from MongoDB `contract_state`
2. Resolves each input tx from `transaction_pool` to get the action and payload
3. Parses router `execute` calls (swap/deposit/withdrawal) and direct calls
4. Inserts into the correct tables (`dex_pool_swap_events`, `dex_pool_add_liq_events`, `dex_pool_rem_liq_events`, `dex_pool_init_events`)
5. Registers the pool in `discovered_contracts` so the live indexer picks up future events

#### Option 2: GraphQL API (no MongoDB access needed)

Uses the VSC GraphQL API. Limited to the ~100 most recent outputs due to API pagination constraints.

```bash
npm install  # needs pg package
node scripts/backfill_dex_pool.js <POOL_CONTRACT_ID> [GRAPHQL_URL] [PG_URL]
```

### Known testnet pools requiring backfill

| Pool Contract | Pair | Notes |
|---|---|---|
| `vsc1BgwiEg8P5u2qYSV7DL8FCqrj5E7hWSYKmf` | BTC/HBD | Old contract, no `pool_init` log. Router: `vsc1BoZJMQqpmdLxUfyRt5Tz82YM7Z57r7Dos7` |

### What gets backfilled

- **init** - Pool initialization (asset pair, fee)
- **swap** / **execute.swap** - Swap events with amounts parsed from payload + `ret` JSON
- **deposit** / **execute.deposit** - Add liquidity events. Amounts from `payload.metadata.amount0`/`amount1`. Note: `lp_minted` is 0 when the contract doesn't return it in `ret`
- **withdrawal** / **execute.withdrawal** - Remove liquidity events. LP burned from `payload.metadata.lp_amount`. Note: returned `amount0`/`amount1` are 0 when the contract doesn't return them in `ret`
- Skipped: `approve`, `register_token`, `migrate`, `update_router` (not indexed)
