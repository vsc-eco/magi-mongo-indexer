/**
 * Backfill script for DEX pool historical data.
 *
 * Queries all successful contract outputs for a given DEX pool contract,
 * resolves each input transaction to determine the action (swap, add_liquidity, etc.),
 * parses the payload and result, and inserts synthetic records into the
 * indexer's PostgreSQL tables.
 *
 * Usage:
 *   node scripts/backfill_dex_pool.js <POOL_CONTRACT_ID> [GRAPHQL_URL] [PG_URL]
 *
 * Example:
 *   node scripts/backfill_dex_pool.js vsc1Brm1QpGF8WXvRCvwgbpB6fiHtTBJzyZUC9
 */

const GRAPHQL_URL = process.argv[3] || "https://magi-test.techcoderx.com/api/v1/graphql";
const PG_URL = process.argv[4] || process.env.DATABASE_URL || "postgres://indexer:indexerpass@localhost:5433/indexerdb";
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

async function getAllSuccessfulOutputs() {
  let allOutputs = [];
  let fromBlock = 0;
  const batchSize = 50;

  while (true) {
    const data = await gql(`
      query($contract: String!, $from: Uint64, $limit: Int) {
        findContractOutput(filterOptions: {
          byContract: $contract,
          fromBlock: $from,
          limit: $limit
        }) {
          id block_height results { ret ok } inputs
        }
      }
    `, { contract: POOL_CONTRACT, from: fromBlock, limit: batchSize });

    const outputs = data.findContractOutput || [];
    if (outputs.length === 0) break;

    const successful = outputs.filter(o => o.results[0].ok);
    allOutputs.push(...successful);

    const maxBlock = Math.max(...outputs.map(o => o.block_height));
    if (outputs.length < batchSize) break;
    fromBlock = maxBlock + 1;
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

function decodePayload(payloadObj) {
  if (!payloadObj || !payloadObj.Data) return null;
  try {
    const decoded = Buffer.from(payloadObj.Data, "base64").toString("utf-8");
    try { return JSON.parse(decoded); } catch { return decoded; }
  } catch { return null; }
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

  // 2. Resolve each tx and build insert statements
  const records = [];
  for (const output of outputs) {
    for (const inputId of output.inputs) {
      const tx = await getTransaction(inputId);
      if (!tx || !tx.ops || !tx.ops[0]) {
        console.log(`  ⚠️  Could not resolve tx ${inputId}`);
        continue;
      }

      const op = tx.ops[0];
      const action = op.data?.action || "unknown";
      const payload = decodePayload(op.data?.payload);

      if (!payload || typeof payload !== "object") {
        console.log(`  ⚠️  Could not decode payload for tx ${inputId} (action: ${action})`);
        continue;
      }

      const ts = new Date(tx.first_seen);
      const bh = output.block_height;

      switch (action) {
        case "swap": {
          let result = {};
          try { result = JSON.parse(output.results[0].ret); } catch {}
          records.push({
            table: "dex_pool_swap_events",
            values: {
              indexer_contract_id: POOL_CONTRACT,
              indexer_tx_hash: tx.id,
              indexer_block_height: bh,
              indexer_ts: ts,
              asset_in: (payload.asset_in || "").toLowerCase(),
              asset_out: (payload.asset_out || "").toLowerCase(),
              amount_in: parseInt(payload.amount_in) || 0,
              amount_out: parseInt(result.amount_out) || 0,
              recipient: payload.recipient || payload.to || "",
            },
          });
          console.log(`  ✅ swap at block ${bh}`);
          break;
        }
        case "add_liquidity": {
          records.push({
            table: "dex_pool_add_liq_events",
            values: {
              indexer_contract_id: POOL_CONTRACT,
              indexer_tx_hash: tx.id,
              indexer_block_height: bh,
              indexer_ts: ts,
              provider: payload.recipient || "",
              amount0: parseInt(payload.amount0) || 0,
              amount1: parseInt(payload.amount1) || 0,
              lp_minted: 0,
            },
          });
          console.log(`  ✅ add_liquidity at block ${bh}`);
          break;
        }
        case "remove_liquidity": {
          records.push({
            table: "dex_pool_rem_liq_events",
            values: {
              indexer_contract_id: POOL_CONTRACT,
              indexer_tx_hash: tx.id,
              indexer_block_height: bh,
              indexer_ts: ts,
              provider: payload.recipient || "",
              amount0: 0,
              amount1: 0,
              lp_burned: parseInt(payload.lp_amount) || 0,
            },
          });
          console.log(`  ✅ remove_liquidity at block ${bh}`);
          break;
        }
        case "init": {
          records.push({
            table: "dex_pool_init_events",
            values: {
              indexer_contract_id: POOL_CONTRACT,
              indexer_tx_hash: tx.id,
              indexer_block_height: bh,
              indexer_ts: ts,
              asset0: (payload.asset0 || "").toLowerCase(),
              asset1: (payload.asset1 || "").toLowerCase(),
              fee_bps: parseInt(payload.fee_bps) || 0,
            },
          });
          console.log(`  ✅ init at block ${bh}`);
          break;
        }
        case "register_pool": {
          records.push({
            table: "dex_router_reg_pool_events",
            values: {
              indexer_contract_id: POOL_CONTRACT,
              indexer_tx_hash: tx.id,
              indexer_block_height: bh,
              indexer_ts: ts,
              asset0: (payload.asset0 || "").toLowerCase(),
              asset1: (payload.asset1 || "").toLowerCase(),
              pool_contract: payload.dex_contract_id || "",
            },
          });
          console.log(`  ✅ register_pool at block ${bh} (${payload.asset0}/${payload.asset1})`);
          break;
        }
        case "register_token": {
          console.log(`  ⏩ Skipping register_token (not indexed): ${payload.name}`);
          break;
        }
        case "approve": {
          console.log(`  ⏩ Skipping approve (not indexed)`);
          break;
        }
        case "execute": {
          // Router-originated tx — parse the inner type
          const execType = payload.type;
          if (execType === "swap") {
            let result = {};
            try { result = JSON.parse(output.results[0].ret); } catch {}
            records.push({
              table: "dex_pool_swap_events",
              values: {
                indexer_contract_id: POOL_CONTRACT,
                indexer_tx_hash: tx.id,
                indexer_block_height: bh,
                indexer_ts: ts,
                asset_in: (payload.asset_in || "").toLowerCase(),
                asset_out: (payload.asset_out || "").toLowerCase(),
                amount_in: parseInt(payload.amount_in) || 0,
                amount_out: parseInt(result.amount_out) || 0,
                recipient: payload.recipient || "",
              },
            });
            console.log(`  ✅ execute/swap at block ${bh}`);
          } else if (execType === "deposit") {
            records.push({
              table: "dex_pool_add_liq_events",
              values: {
                indexer_contract_id: POOL_CONTRACT,
                indexer_tx_hash: tx.id,
                indexer_block_height: bh,
                indexer_ts: ts,
                provider: payload.recipient || "",
                amount0: parseInt(payload.amount0) || 0,
                amount1: parseInt(payload.amount1) || 0,
                lp_minted: 0,
              },
            });
            console.log(`  ✅ execute/deposit at block ${bh}`);
          } else if (execType === "withdrawal") {
            records.push({
              table: "dex_pool_rem_liq_events",
              values: {
                indexer_contract_id: POOL_CONTRACT,
                indexer_tx_hash: tx.id,
                indexer_block_height: bh,
                indexer_ts: ts,
                provider: payload.recipient || "",
                amount0: 0,
                amount1: 0,
                lp_burned: parseInt(payload.lp_amount) || 0,
              },
            });
            console.log(`  ✅ execute/withdrawal at block ${bh}`);
          } else {
            console.log(`  ⏩ Skipping execute type: ${execType} (tx: ${inputId.slice(0, 16)}...)`);
          }
          break;
        }
        default:
          console.log(`  ⏩ Skipping: ${action} (tx: ${inputId.slice(0, 16)}...)`);
      }
    }
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
    console.log("⚠️  'pg' package not installed. Install with: npm install pg");
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

    for (const r of records) {
      const cols = Object.keys(r.values);
      const vals = Object.values(r.values);
      const placeholders = cols.map((_, i) => `$${i + 1}`).join(", ");

      const sql = `
        INSERT INTO ${r.table} (${cols.join(", ")})
        VALUES (${placeholders})
      `;

      try {
        // Check for duplicates first
        const check = await client.query(
          `SELECT 1 FROM ${r.table} WHERE indexer_tx_hash = $1 LIMIT 1`,
          [r.values.indexer_tx_hash]
        );
        if (check.rowCount > 0) {
          console.log(`  ⏩ Already exists in ${r.table} (tx: ${r.values.indexer_tx_hash.slice(0, 16)}...)`);
          continue;
        }

        const result = await client.query(sql, vals);
        console.log(`  ✅ Inserted into ${r.table} (tx: ${r.values.indexer_tx_hash.slice(0, 16)}...)`);
        void result;
      } catch (err) {
        console.log(`  ❌ Error inserting into ${r.table}: ${err.message}`);
      }
    }

    console.log("\n✅ Backfill complete!");
  } finally {
    await client.end();
  }
}

main().catch(err => {
  console.error("Fatal error:", err.message);
  process.exit(1);
});
