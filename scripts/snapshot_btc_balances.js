/**
 * Snapshot current BTC mapping balances from contract state into PostgreSQL.
 *
 * Queries all known account balances from the contract's state (a-<account> keys)
 * and inserts synthetic deposit records for accounts not yet in the database.
 * This bootstraps the balance view for accounts that existed before logging was added.
 *
 * Usage:
 *   node scripts/snapshot_btc_balances.js [ACCOUNTS_COMMA_SEPARATED] [GRAPHQL_URL] [PG_URL]
 *
 * Examples:
 *   # Specific accounts:
 *   node scripts/snapshot_btc_balances.js "hive:tibfox,hive:sagar,hive:milo-hpr"
 *
 *   # Auto-discover from ledger txs:
 *   node scripts/snapshot_btc_balances.js auto
 */

const CONTRACT_ID = "vsc1BYBwMvsSFwqvwzio352VWp6fGkjVs7t3Dp";
const GRAPHQL_URL = process.argv[3] || "https://magi-test.techcoderx.com/api/v1/graphql";
const PG_URL = process.argv[4] || process.env.DATABASE_URL || "postgres://indexer:indexerpass@127.0.0.1:5433/indexerdb";
const ACCOUNTS_ARG = process.argv[2] || "auto";

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

async function discoverAccounts() {
  // Find accounts that have interacted with the BTC mapping contract
  // by checking ledger transfers and contract outputs
  const accounts = new Set();

  // Check ledger for transfers involving the contract owner
  const data = await gql(`{
    findTransaction(filterOptions: {
      byContract: "${CONTRACT_ID}",
      byStatus: CONFIRMED,
      limit: 100
    }) {
      id required_auths ops { data }
    }
  }`);

  for (const tx of data.findTransaction || []) {
    for (const auth of tx.required_auths || []) {
      if (auth.startsWith("hive:")) accounts.add(auth);
    }
  }

  // Also check common known accounts
  const knownAccounts = [
    "hive:tibfox", "hive:sagar", "hive:milo-hpr",
    "hive:milo.magi", "hive:magi.contracts", "hive:magi.testdev",
  ];
  for (const a of knownAccounts) accounts.add(a);

  return [...accounts];
}

async function getBalances(accounts) {
  const keys = accounts.map(a => `a-${a}`);
  const data = await gql(`
    query($contractId: String!, $keys: [String!]!) {
      getStateByKeys(contractId: $contractId, keys: $keys, encoding: "hex")
    }
  `, { contractId: CONTRACT_ID, keys });

  const balances = [];
  const stateData = data.getStateByKeys;
  for (const [key, hexVal] of Object.entries(stateData)) {
    if (!hexVal) continue;
    const account = key.replace("a-", "");
    const sats = parseInt(hexVal, 16);
    if (sats > 0) {
      balances.push({ account, sats });
    }
  }
  return balances;
}

async function main() {
  console.log(`Snapshotting BTC balances from: ${CONTRACT_ID}`);
  console.log(`GraphQL: ${GRAPHQL_URL}`);
  console.log(`Postgres: ${PG_URL.replace(/:[^:@]+@/, ":***@")}\n`);

  // 1. Get accounts
  let accounts;
  if (ACCOUNTS_ARG === "auto") {
    console.log("Auto-discovering accounts...");
    accounts = await discoverAccounts();
  } else {
    accounts = ACCOUNTS_ARG.split(",").map(a => a.trim());
  }
  console.log(`Checking ${accounts.length} accounts: ${accounts.join(", ")}\n`);

  // 2. Get balances from contract state
  const balances = await getBalances(accounts);
  console.log(`Found ${balances.length} accounts with balances:`);
  for (const b of balances) {
    console.log(`  ${b.account}: ${b.sats} sats (${(b.sats / 1e8).toFixed(8)} BTC)`);
  }

  if (balances.length === 0) {
    console.log("\nNo balances to snapshot.");
    return;
  }

  // 3. Insert into PostgreSQL
  let pg;
  try { pg = require("pg"); } catch {
    console.log("\n⚠️  'pg' package not installed. Install with: npm install pg");
    return;
  }

  const client = new pg.Client(PG_URL);
  try {
    await client.connect();
    console.log("\nConnected to PostgreSQL\n");

    // First check what's already in the DB for this account
    for (const b of balances) {
      const txHash = `snapshot-${b.account}`;

      // Check existing snapshot
      const existing = await client.query(
        "SELECT 1 FROM btc_mapping_deposit_events WHERE indexer_tx_hash = $1 LIMIT 1",
        [txHash]
      );

      if (existing.rowCount > 0) {
        // Update the snapshot amount in case balance changed
        await client.query(
          "UPDATE btc_mapping_deposit_events SET amount = $1 WHERE indexer_tx_hash = $2",
          [b.sats, txHash]
        );
        console.log(`  🔄 Updated snapshot for ${b.account}: ${b.sats} sats`);
      } else {
        // Check if there are already real deposit records for this account
        const realDeposits = await client.query(
          "SELECT COALESCE(SUM(amount), 0) AS total FROM btc_mapping_deposit_events WHERE recipient = $1 AND indexer_tx_hash NOT LIKE 'snapshot-%' AND indexer_tx_hash NOT LIKE 'backfill-%'",
          [b.account]
        );
        const existingTotal = parseInt(realDeposits.rows[0].total) || 0;

        if (existingTotal >= b.sats) {
          console.log(`  ⏩ ${b.account}: real deposits (${existingTotal}) >= state balance (${b.sats}), skipping`);
          continue;
        }

        // Insert the difference as a snapshot record
        const snapshotAmount = b.sats - existingTotal;
        await client.query(
          `INSERT INTO btc_mapping_deposit_events
           (indexer_contract_id, indexer_tx_hash, indexer_block_height, indexer_ts, recipient, sender, amount)
           VALUES ($1, $2, 0, NOW(), $3, 'snapshot', $4)`,
          [CONTRACT_ID, txHash, b.account, snapshotAmount]
        );
        console.log(`  ✅ Inserted snapshot for ${b.account}: ${snapshotAmount} sats (adjusted for ${existingTotal} existing)`);
      }
    }

    // Clean up old backfill records if snapshot replaces them
    const cleaned = await client.query(
      "DELETE FROM btc_mapping_deposit_events WHERE indexer_tx_hash LIKE 'backfill-%'"
    );
    if (cleaned.rowCount > 0) {
      console.log(`\n  🧹 Cleaned ${cleaned.rowCount} old backfill records`);
    }

    console.log("\n✅ Snapshot complete!");
  } finally {
    await client.end();
  }
}

main().catch(err => {
  console.error("Fatal error:", err.message);
  process.exit(1);
});
