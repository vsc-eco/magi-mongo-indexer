-- =============================================================================
-- Backfill: repopulate dex_pool_fee_events rows left EMPTY by the old config
-- =============================================================================
--
-- Context
-- -------
-- The dex `fee` log was rewritten on-chain to the 8-token pendulum layout
-- (dex-contracts commit 52ab2a4 "use new pendulum sdk method"):
--
--   fee|a=<asset>|lp=<lp>|ns=<ns>|nc=<nc>|nb=<nb>|m=<m>|s=<s>
--
-- Indexers running the pre-fix config only knew the 4- and 5-token fee
-- variants, so the parser produced an empty field map for these logs. The row
-- was still inserted (the core indexer_* columns are always written) but every
-- data column stayed NULL. Those rows show up as `asset IS NULL` despite the
-- raw log being a full 8-token pendulum line.
--
-- The companion config fix (dex_pool_mappings.yaml `field_count: 8` variant)
-- makes NEW logs parse correctly, but it does not retroactively touch rows that
-- were already stored empty. This script fixes that history in place.
--
-- What it does
-- ------------
-- For every dex_pool_fee_events row that is still empty (asset IS NULL) whose
-- raw log in contract_logs is the 8-token pendulum layout, it re-parses the
-- log and fills the columns. total_fee/magi_fee are intentionally left NULL —
-- the pendulum layout does not emit them and the dex_pool_* views derive them
-- (total = lp+ns+nc, protocol/magi = ns+nc). See dex_pool_mappings.yaml for the
-- authoritative position->column map; keep the split_part offsets below in sync.
--
-- Linkage: dex_pool_fee_events.indexer_log_hash = sha256("<block>:<tx>:<log>"),
-- exactly as datalayer.BuildInsertSQL computes it, so each empty row is matched
-- to its one originating raw log with no ambiguity.
--
-- Safe to re-run: the `asset IS NULL` guard means a second pass updates nothing.
-- Only the 8-token pendulum layout is touched; legacy 4/5-token rows (already
-- populated) are never modified.
--
-- Usage
-- -----
--   psql "<DSN>" -v ON_ERROR_STOP=1 -f scripts/backfill_dex_fees.sql
-- or via the wrapper:
--   scripts/backfill_dex_fees.sh [PG_CONTAINER]
--
-- The in-process alternative is to restart the indexer with BACKFILL_BLOCKS set
-- large enough to cover the pendulum-era blocks; this script needs no restart.
-- =============================================================================

\set ON_ERROR_STOP on

BEGIN;

-- sha256() / digest() live in pgcrypto. The indexer DB owner can create it.
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Make the script self-sufficient even if the config has not hot-reloaded yet
-- (mirrors the datalayer's ALTER TABLE ADD COLUMN IF NOT EXISTS pass).
ALTER TABLE dex_pool_fee_events ADD COLUMN IF NOT EXISTS node_share      NUMERIC;
ALTER TABLE dex_pool_fee_events ADD COLUMN IF NOT EXISTS network_credit  NUMERIC;
ALTER TABLE dex_pool_fee_events ADD COLUMN IF NOT EXISTS node_bucket_hbd NUMERIC;
ALTER TABLE dex_pool_fee_events ADD COLUMN IF NOT EXISTS multiplier_bps  NUMERIC;
ALTER TABLE dex_pool_fee_events ADD COLUMN IF NOT EXISTS s_bps           NUMERIC;

-- How many empty pendulum rows are about to be fixed.
SELECT count(*) AS empty_pendulum_rows_before
FROM dex_pool_fee_events f
JOIN contract_logs cl
  ON f.indexer_log_hash =
     encode(digest(cl.block_height::text || ':' || cl.tx_hash || ':' || cl.log, 'sha256'), 'hex')
WHERE f.asset IS NULL
  AND cl.log LIKE 'fee|%'
  AND (length(cl.log) - length(replace(cl.log, '|', ''))) + 1 = 8;

-- Re-parse and fill. Positions match the field_count:8 variant in
-- dex_pool_mappings.yaml: a=2 lp=3 ns=4 nc=5 nb=6 m=7 s=8 (split_part is
-- 1-indexed; token 1 is the "fee" prefix).
UPDATE dex_pool_fee_events AS f
SET
  asset           = split_part(split_part(cl.log, '|', 2), '=', 2),
  lp_fee          = NULLIF(split_part(split_part(cl.log, '|', 3), '=', 2), '')::numeric,
  node_share      = NULLIF(split_part(split_part(cl.log, '|', 4), '=', 2), '')::numeric,
  network_credit  = NULLIF(split_part(split_part(cl.log, '|', 5), '=', 2), '')::numeric,
  node_bucket_hbd = NULLIF(split_part(split_part(cl.log, '|', 6), '=', 2), '')::numeric,
  multiplier_bps  = NULLIF(split_part(split_part(cl.log, '|', 7), '=', 2), '')::numeric,
  s_bps           = NULLIF(split_part(split_part(cl.log, '|', 8), '=', 2), '')::numeric
FROM contract_logs AS cl
WHERE f.indexer_log_hash =
      encode(digest(cl.block_height::text || ':' || cl.tx_hash || ':' || cl.log, 'sha256'), 'hex')
  AND f.asset IS NULL
  AND cl.log LIKE 'fee|%'
  AND (length(cl.log) - length(replace(cl.log, '|', ''))) + 1 = 8;

-- Remaining empty pendulum rows (expected: 0).
SELECT count(*) AS empty_pendulum_rows_after
FROM dex_pool_fee_events f
JOIN contract_logs cl
  ON f.indexer_log_hash =
     encode(digest(cl.block_height::text || ':' || cl.tx_hash || ':' || cl.log, 'sha256'), 'hex')
WHERE f.asset IS NULL
  AND cl.log LIKE 'fee|%'
  AND (length(cl.log) - length(replace(cl.log, '|', ''))) + 1 = 8;

COMMIT;
