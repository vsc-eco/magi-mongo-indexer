/**
 * Full historical backfill for DEX pool data, reading directly from MongoDB.
 *
 * Unlike backfill_dex_pool.js (which uses the GraphQL API and is limited to
 * ~100 most recent outputs), this script reads all contract_state documents
 * from MongoDB and resolves input transactions from the transaction_pool
 * collection. This gives complete coverage of all historical events.
 *
 * Handles:
 * - Direct contract calls (swap, add_liquidity, remove_liquidity, init)
 * - Router "execute" calls with nested payload (swap, deposit, withdrawal)
 * - BSON Binary payloads (double-encoded JSON)
 * - Payload metadata for deposit amounts and withdrawal LP amounts
 *
 * Usage:
 *   node scripts/backfill_dex_pool_mongo.js <POOL_CONTRACT_ID> [MONGO_URI] [PG_URL]
 *
 * Example:
 *   node scripts/backfill_dex_pool_mongo.js vsc1BgwiEg8P5u2qYSV7DL8FCqrj5E7hWSYKmf
 *
 * Requires: npm install pg mongodb
 */

const POOL_CONTRACT = process.argv[2];
const MONGO_URI = process.argv[3] || process.env.MONGO_URI || "mongodb://localhost:27017";
const MONGO_DB = process.env.MONGO_DB_NAME || "go-vsc";
const PG_URL = process.argv[4] || process.env.DATABASE_URL || "postgres://indexer:indexerpass@127.0.0.1:5433/indexerdb";

if (!POOL_CONTRACT) {
  console.error("Usage: node backfill_dex_pool_mongo.js <POOL_CONTRACT_ID> [MONGO_URI] [PG_URL]");
  process.exit(1);
}

function parsePayload(raw) {
  if (!raw) return null;

  // Plain JSON string
  if (typeof raw === "string") {
    try { return JSON.parse(raw); } catch { return null; }
  }

  // BSON Binary (has .buffer property)
  if (raw.buffer) {
    const s = raw.buffer.toString("utf8");
    try {
      const parsed = JSON.parse(s);
      return typeof parsed === "string" ? JSON.parse(parsed) : parsed;
    } catch { return null; }
  }

  // Base64 object ({ Data: "..." })
  if (raw.Data) {
    try {
      const decoded = Buffer.from(raw.Data, "base64").toString("utf-8");
      try {
        const parsed = JSON.parse(decoded);
        return typeof parsed === "string" ? JSON.parse(parsed) : parsed;
      } catch { return decoded; }
    } catch { return null; }
  }

  // Plain object
  if (typeof raw === "object") return raw;
  return null;
}

function getEffectiveAction(action, payload) {
  if (action === "execute" && payload?.type) return payload.type;
  return action;
}

async function main() {
  console.log(`Backfilling DEX pool: ${POOL_CONTRACT}`);
  console.log(`MongoDB: ${MONGO_URI} / ${MONGO_DB}`);
  console.log(`Postgres: ${PG_URL.replace(/:[^:@]+@/, ":***@")}`);
  console.log();

  const { MongoClient } = require("mongodb");
  const pg = require("pg");

  const mongo = new MongoClient(MONGO_URI);
  const pgClient = new pg.Client(PG_URL);

  try {
    await mongo.connect();
    await pgClient.connect();

    const db = mongo.db(MONGO_DB);
    const contractState = db.collection("contract_state");
    const txPool = db.collection("transaction_pool");

    // 1. Fetch all successful outputs for this contract
    console.log("Fetching contract outputs from MongoDB...");
    const docs = await contractState.find({
      contract_id: POOL_CONTRACT,
      "results.0.ok": true,
    }).sort({ block_height: 1 }).toArray();

    console.log(`Found ${docs.length} successful outputs\n`);

    // 2. Register pool in discovered_contracts
    await pgClient.query(
      `INSERT INTO discovered_contracts (contract_id, discover_event, block_height)
       VALUES ($1, 'pool_init', 0)
       ON CONFLICT DO NOTHING`,
      [POOL_CONTRACT]
    );

    // 3. Process each output
    let inserted = 0, skippedDup = 0, skippedOther = 0, errors = 0;
    const actionCounts = {};

    for (const doc of docs) {
      for (const inp of (doc.inputs || [])) {
        const txId = inp.split("-")[0];
        const tx = await txPool.findOne({ id: txId });
        if (!tx || !tx.ops || !tx.ops[0]) continue;

        const op = tx.ops[0];
        const rawAction = op.data?.action || "unknown";
        const payload = parsePayload(op.data?.payload);
        if (!payload || typeof payload !== "object") continue;

        const action = getEffectiveAction(rawAction, payload);
        actionCounts[action] = (actionCounts[action] || 0) + 1;

        let ret = {};
        try { ret = JSON.parse(doc.results[0].ret); } catch {}
        const ts = tx.first_seen ? new Date(tx.first_seen) : new Date(0);
        const bh = Number(doc.block_height);

        let table, values;

        switch (action) {
          case "swap": {
            table = "dex_pool_swap_events";
            values = {
              indexer_contract_id: POOL_CONTRACT,
              indexer_tx_hash: txId,
              indexer_block_height: bh,
              indexer_ts: ts,
              asset_in: (payload.asset_in || "").toLowerCase(),
              asset_out: (payload.asset_out || "").toLowerCase(),
              amount_in: parseInt(payload.amount_in) || 0,
              amount_out: parseInt(ret.amount_out) || 0,
              recipient: payload.recipient || payload.to || "",
            };
            break;
          }
          case "deposit":
          case "add_liquidity": {
            const meta = payload.metadata || {};
            table = "dex_pool_add_liq_events";
            values = {
              indexer_contract_id: POOL_CONTRACT,
              indexer_tx_hash: txId,
              indexer_block_height: bh,
              indexer_ts: ts,
              provider: payload.recipient || tx.required_auths?.[0] || "",
              amount0: parseInt(meta.amount0 || payload.amount0) || 0,
              amount1: parseInt(meta.amount1 || payload.amount1) || 0,
              lp_minted: parseInt(ret.lp_minted) || 0,
            };
            break;
          }
          case "withdrawal":
          case "remove_liquidity": {
            const meta = payload.metadata || {};
            table = "dex_pool_rem_liq_events";
            values = {
              indexer_contract_id: POOL_CONTRACT,
              indexer_tx_hash: txId,
              indexer_block_height: bh,
              indexer_ts: ts,
              provider: payload.recipient || tx.required_auths?.[0] || "",
              amount0: parseInt(ret.amount0) || 0,
              amount1: parseInt(ret.amount1) || 0,
              lp_burned: parseInt(meta.lp_amount || payload.lp_amount) || 0,
            };
            break;
          }
          case "init": {
            table = "dex_pool_init_events";
            values = {
              indexer_contract_id: POOL_CONTRACT,
              indexer_tx_hash: txId,
              indexer_block_height: bh,
              indexer_ts: ts,
              asset0: (payload.asset0 || "").toLowerCase(),
              asset1: (payload.asset1 || "").toLowerCase(),
              fee_bps: parseInt(payload.fee_bps) || 0,
            };
            break;
          }
          default:
            skippedOther++;
            continue;
        }

        // Insert with dedup
        try {
          const check = await pgClient.query(
            `SELECT 1 FROM ${table} WHERE indexer_tx_hash = $1 AND indexer_contract_id = $2 LIMIT 1`,
            [txId, POOL_CONTRACT]
          );
          if (check.rowCount > 0) {
            skippedDup++;
            continue;
          }

          const cols = Object.keys(values);
          const vals = Object.values(values);
          const placeholders = cols.map((_, i) => `$${i + 1}`).join(", ");
          await pgClient.query(
            `INSERT INTO ${table} (${cols.join(", ")}) VALUES (${placeholders})`,
            vals
          );
          inserted++;
        } catch (err) {
          errors++;
          console.log(`  ❌ ${table}: ${err.message}`);
        }
      }
    }

    console.log("Action breakdown:", actionCounts);
    console.log(`\n✅ Backfill complete: ${inserted} inserted, ${skippedDup} duplicates skipped, ${skippedOther} non-indexed skipped, ${errors} errors`);

  } finally {
    await mongo.close();
    await pgClient.end();
  }
}

main().catch(err => {
  console.error("Fatal error:", err.message);
  process.exit(1);
});
