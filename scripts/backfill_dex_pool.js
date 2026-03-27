/**
 * Backfill script for DEX pool historical data.
 *
 * Queries all successful contract outputs for a given DEX pool contract,
 * resolves each input transaction to determine the action (swap, add_liquidity, etc.),
 * parses the payload and result, and inserts records into the indexer's PostgreSQL tables.
 *
 * Also registers the pool in discovered_contracts if not already present.
 *
 * Handles both direct contract calls and router "execute" calls where the
 * actual action (swap/deposit/withdrawal) is nested inside the payload.
 * Supports payloads as plain JSON strings, base64-encoded objects, or BSON Binary.
 *
 * NOTE: The VSC GraphQL API returns results in descending block order with no
 * offset/cursor support. This limits the script to the most recent ~100 outputs.
 * For a full historical backfill on pools with >100 outputs, use
 * backfill_dex_pool_mongo.js which reads directly from MongoDB.
 *
 * Usage:
 *   node scripts/backfill_dex_pool.js <POOL_CONTRACT_ID> [GRAPHQL_URL] [PG_URL]
 *
 * Example:
 *   node scripts/backfill_dex_pool.js vsc1BgwiEg8P5u2qYSV7DL8FCqrj5E7hWSYKmf
 */

const GRAPHQL_URL = process.argv[3] || "https://magi-test.techcoderx.com/api/v1/graphql";
const PG_URL = process.argv[4] || process.env.DATABASE_URL || "postgres://indexer:indexerpass@127.0.0.1:5433/indexerdb";
const POOL_CONTRACT = process.argv[2];

if (!POOL_CONTRACT) {
  console.error("Usage: node backfill_dex_pool.js <POOL_CONTRACT_ID> [GRAPHQL_URL] [PG_URL]");
  process.exit(1);
}

async function gql(query, variables = {}) {
  const res = await fetch(GRAPHQL_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query, variables }),
  });
  const json = await res.json();
  if (json.errors) throw new Error(JSON.stringify(json.errors));
  return json.data;
}

/**
 * Fetches all successful contract outputs, paginating by collecting seen IDs
 * since the API returns results in descending block order and fromBlock
 * doesn't reliably filter.
 */
async function getAllSuccessfulOutputs() {
  const allOutputs = [];
  const seenIds = new Set();
  const batchSize = 100;
  let prevCount = -1;

  while (true) {
    const data = await gql(`
      query($contract: String!, $limit: Int) {
        findContractOutput(filterOptions: {
          byContract: $contract,
          limit: $limit
        }) {
          id block_height results { ret ok } inputs
        }
      }
    `, { contract: POOL_CONTRACT, limit: batchSize });

    const outputs = data.findContractOutput || [];
    if (outputs.length === 0) break;

    let newCount = 0;
    for (const o of outputs) {
      if (!seenIds.has(o.id) && o.results[0]?.ok) {
        seenIds.add(o.id);
        allOutputs.push(o);
        newCount++;
      }
    }

    // API returns max `limit` results in descending order with no offset.
    // If we got fewer than batchSize, we've seen everything.
    // If no new results were added, we're stuck on the same page — done.
    if (outputs.length < batchSize || newCount === 0) break;

    // Guard against infinite loops if pagination doesn't advance
    if (allOutputs.length === prevCount) break;
    prevCount = allOutputs.length;
  }

  return allOutputs;
}

async function getTransaction(txId) {
  const data = await gql(`
    query($id: String!) {
      findTransaction(filterOptions: { byId: $id }) {
        id first_seen required_auths ops { type data }
      }
    }
  `, { id: txId });
  return data.findTransaction?.[0] || null;
}

/**
 * Decodes a transaction payload from any format:
 * - Plain JSON string (router execute calls via GraphQL API)
 * - Base64 object ({ Data: "..." }) (older direct calls)
 * - Plain object (already parsed)
 */
function decodePayload(payloadObj) {
  if (!payloadObj) return null;

  // Plain JSON string
  if (typeof payloadObj === "string") {
    try { return JSON.parse(payloadObj); } catch { return null; }
  }

  // Base64-encoded object ({ Data: "..." })
  if (payloadObj.Data) {
    try {
      const decoded = Buffer.from(payloadObj.Data, "base64").toString("utf-8");
      // May be double-encoded (stringified JSON inside base64)
      try {
        const parsed = JSON.parse(decoded);
        return typeof parsed === "string" ? JSON.parse(parsed) : parsed;
      } catch {
        return decoded;
      }
    } catch { return null; }
  }

  // Plain object
  if (typeof payloadObj === "object") return payloadObj;
  return null;
}

/**
 * Determines the effective action type, resolving router "execute" wrappers.
 */
function getEffectiveAction(action, payload) {
  if (action === "execute" && payload?.type) return payload.type;
  return action;
}

function buildRecord(action, payload, ret, tx, output) {
  const ts = new Date(tx.first_seen);
  const bh = output.block_height;
  const base = { indexer_contract_id: POOL_CONTRACT, indexer_tx_hash: tx.id, indexer_block_height: bh, indexer_ts: ts };

  switch (action) {
    case "swap": {
      return {
        table: "dex_pool_swap_events",
        values: {
          ...base,
          asset_in: (payload.asset_in || "").toLowerCase(),
          asset_out: (payload.asset_out || "").toLowerCase(),
          amount_in: parseInt(payload.amount_in) || 0,
          amount_out: parseInt(ret.amount_out) || 0,
          recipient: payload.recipient || payload.to || "",
        },
      };
    }
    case "deposit":
    case "add_liquidity": {
      // Amounts may be in payload.metadata (router execute) or payload directly
      const meta = payload.metadata || {};
      return {
        table: "dex_pool_add_liq_events",
        values: {
          ...base,
          provider: payload.recipient || tx.required_auths?.[0] || "",
          amount0: parseInt(meta.amount0 || payload.amount0) || 0,
          amount1: parseInt(meta.amount1 || payload.amount1) || 0,
          lp_minted: parseInt(ret.lp_minted) || 0,
        },
      };
    }
    case "withdrawal":
    case "remove_liquidity": {
      const meta = payload.metadata || {};
      return {
        table: "dex_pool_rem_liq_events",
        values: {
          ...base,
          provider: payload.recipient || tx.required_auths?.[0] || "",
          amount0: parseInt(ret.amount0) || 0,
          amount1: parseInt(ret.amount1) || 0,
          lp_burned: parseInt(meta.lp_amount || payload.lp_amount) || 0,
        },
      };
    }
    case "init": {
      return {
        table: "dex_pool_init_events",
        values: {
          ...base,
          asset0: (payload.asset0 || "").toLowerCase(),
          asset1: (payload.asset1 || "").toLowerCase(),
          fee_bps: parseInt(payload.fee_bps) || 0,
        },
      };
    }
    case "register_pool": {
      return {
        table: "dex_router_reg_pool_events",
        values: {
          ...base,
          asset0: (payload.asset0 || "").toLowerCase(),
          asset1: (payload.asset1 || "").toLowerCase(),
          pool_contract: payload.dex_contract_id || "",
        },
      };
    }
    default:
      return null;
  }
}

async function main() {
  console.log(`Backfilling DEX pool: ${POOL_CONTRACT}`);
  console.log(`GraphQL: ${GRAPHQL_URL}`);
  console.log(`Postgres: ${PG_URL.replace(/:[^:@]+@/, ":***@")}`);
  console.log();

  // 1. Fetch all successful outputs
  console.log("Fetching successful contract outputs...");
  const outputs = await getAllSuccessfulOutputs();
  console.log(`Found ${outputs.length} successful outputs\n`);

  // 2. Resolve each tx and build insert records
  const records = [];
  const skipped = { approve: 0, register_token: 0, migrate: 0, update_router: 0, unknown: 0 };

  for (const output of outputs) {
    for (const inputId of output.inputs) {
      const cleanId = inputId.split("-")[0];
      const tx = await getTransaction(cleanId);
      if (!tx || !tx.ops || !tx.ops[0]) {
        console.log(`  ⚠️  Could not resolve tx ${inputId}`);
        continue;
      }

      const op = tx.ops[0];
      const rawAction = op.data?.action || "unknown";
      const payload = decodePayload(op.data?.payload);

      if (!payload || typeof payload !== "object") {
        console.log(`  ⚠️  Could not decode payload for tx ${inputId} (action: ${rawAction})`);
        continue;
      }

      const action = getEffectiveAction(rawAction, payload);
      let ret = {};
      try { ret = JSON.parse(output.results[0].ret); } catch {}

      const record = buildRecord(action, payload, ret, tx, output);

      if (record) {
        records.push(record);
        console.log(`  ✅ ${action} at block ${output.block_height}`);
      } else if (skipped[action] !== undefined) {
        skipped[action]++;
      } else {
        skipped.unknown++;
        console.log(`  ⏩ Skipping: ${action} (tx: ${inputId.slice(0, 16)}...)`);
      }
    }
  }

  for (const [action, count] of Object.entries(skipped)) {
    if (count > 0) console.log(`  ⏩ Skipped ${count} ${action} events (not indexed)`);
  }

  console.log(`\nPrepared ${records.length} records for insertion\n`);

  if (records.length === 0) {
    console.log("Nothing to insert.");
    return;
  }

  // 3. Insert into PostgreSQL
  let pg;
  try {
    pg = require("pg");
  } catch {
    console.log("'pg' package not installed. Install with: npm install pg");
    console.log("\nRecords that would be inserted:");
    for (const r of records) {
      console.log(`  ${r.table}: ${JSON.stringify(r.values).slice(0, 120)}...`);
    }
    return;
  }

  const client = new pg.Client(PG_URL);
  try {
    await client.connect();
    console.log("Connected to PostgreSQL\n");

    // Register pool in discovered_contracts if not present
    await client.query(
      `INSERT INTO discovered_contracts (contract_id, discover_event, block_height)
       VALUES ($1, 'pool_init', 0)
       ON CONFLICT DO NOTHING`,
      [POOL_CONTRACT]
    );

    let inserted = 0, skippedDup = 0, errors = 0;

    for (const r of records) {
      const cols = Object.keys(r.values);
      const vals = Object.values(r.values);
      const placeholders = cols.map((_, i) => `$${i + 1}`).join(", ");

      try {
        // Check for duplicates
        const check = await client.query(
          `SELECT 1 FROM ${r.table} WHERE indexer_tx_hash = $1 AND indexer_contract_id = $2 LIMIT 1`,
          [r.values.indexer_tx_hash, POOL_CONTRACT]
        );
        if (check.rowCount > 0) {
          skippedDup++;
          continue;
        }

        await client.query(
          `INSERT INTO ${r.table} (${cols.join(", ")}) VALUES (${placeholders})`,
          vals
        );
        inserted++;
      } catch (err) {
        errors++;
        console.log(`  ❌ Error inserting into ${r.table}: ${err.message}`);
      }
    }

    console.log(`\n✅ Backfill complete: ${inserted} inserted, ${skippedDup} duplicates skipped, ${errors} errors`);
  } finally {
    await client.end();
  }
}

main().catch(err => {
  console.error("Fatal error:", err.message);
  process.exit(1);
});
