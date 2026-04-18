// Package backfill re-parses raw logs from the contract_logs table and
// updates event-table rows whose schema columns are NULL. It is meant to
// run once on startup, bounded by a small block window, so recent rows
// affected by a mapping change (new column, new variant) can be fixed
// without a full re-index.
package backfill

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"strings"

	"github.com/tibfox/magi-mongo-indexer/internal/indexer/mapper"
	"github.com/tibfox/magi-mongo-indexer/internal/indexer/parser"
	"github.com/tibfox/magi-mongo-indexer/internal/indexer/types"
)

// Run scans the last `blocks` block heights of raw logs per contract,
// re-parses each with the current mapping, and UPDATEs event rows that
// still have NULLs. Correctly-parsed rows are never touched (the UPDATE
// carries a `col IS NULL OR ...` guard).
func Run(db *sql.DB, mappings *types.MappingFile, blocks uint64) error {
	if blocks == 0 {
		return nil
	}

	// Build addr → events, covering both static and discovered contracts.
	contractEvents := map[string][]types.EventMapping{}
	for _, c := range mappings.Contracts {
		if c.Address != "" {
			contractEvents[c.Address] = append(contractEvents[c.Address], c.Events...)
			continue
		}
		if c.DiscoverEvent == "" {
			continue
		}
		for _, addr := range mapper.GetDiscoveredContracts(c.DiscoverEvent) {
			contractEvents[addr] = append(contractEvents[addr], c.Events...)
		}
	}

	total := 0
	for addr, events := range contractEvents {
		fixed, err := runContract(db, addr, events, blocks)
		if err != nil {
			log.Printf("[backfill] error for %s: %v", addr, err)
			continue
		}
		total += fixed
	}
	log.Printf("[backfill] complete — %d row(s) updated across %d contract(s), lookback=%d blocks",
		total, len(contractEvents), blocks)
	return nil
}

func runContract(db *sql.DB, addr string, events []types.EventMapping, blocks uint64) (int, error) {
	var maxBlock sql.NullInt64
	if err := db.QueryRow(
		`SELECT MAX(block_height) FROM contract_logs WHERE contract_address = $1`,
		addr,
	).Scan(&maxBlock); err != nil {
		return 0, err
	}
	if !maxBlock.Valid {
		return 0, nil
	}
	fromBlock := uint64(0)
	if uint64(maxBlock.Int64) > blocks {
		fromBlock = uint64(maxBlock.Int64) - blocks
	}

	rows, err := db.Query(
		`SELECT block_height, tx_hash, log FROM contract_logs
		 WHERE contract_address = $1 AND block_height >= $2`,
		addr, fromBlock,
	)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	fixed := 0
	for rows.Next() {
		var blockHeight uint64
		var txHash, logStr string
		if err := rows.Scan(&blockHeight, &txHash, &logStr); err != nil {
			continue
		}

		var match *types.EventMapping
		for i := range events {
			if mapper.MatchesLogType(events[i], logStr) {
				match = &events[i]
				break
			}
		}
		if match == nil {
			continue
		}

		parsed := parser.ParseLog(*match, logStr)
		if len(parsed) == 0 {
			continue
		}

		// indexer_log_hash is sha256("block:tx:log") — same construction
		// as datalayer.BuildInsertSQL uses on insert.
		hashInput := fmt.Sprintf("%d:%s:%s", blockHeight, txHash, logStr)
		hashBytes := sha256.Sum256([]byte(hashInput))
		logHash := hex.EncodeToString(hashBytes[:])

		if n, err := update(db, *match, logHash, parsed); err != nil {
			log.Printf("[backfill] update failed for %s log_hash=%s: %v", match.Table, logHash, err)
		} else if n > 0 {
			fixed++
		}
	}
	if err := rows.Err(); err != nil {
		return fixed, err
	}
	return fixed, nil
}

func update(db *sql.DB, m types.EventMapping, logHash string, parsed map[string]interface{}) (int64, error) {
	setClauses := make([]string, 0, len(parsed))
	nullGuards := make([]string, 0, len(m.Schema))
	vals := make([]interface{}, 0, len(parsed)+1)

	i := 1
	for col, v := range parsed {
		setClauses = append(setClauses, fmt.Sprintf("%s = $%d", quoteIdent(col), i))
		vals = append(vals, v)
		i++
	}
	if len(setClauses) == 0 {
		return 0, nil
	}
	for col := range m.Schema {
		nullGuards = append(nullGuards, fmt.Sprintf("%s IS NULL", quoteIdent(col)))
	}

	vals = append(vals, logHash)
	stmt := fmt.Sprintf(
		`UPDATE %s SET %s WHERE indexer_log_hash = $%d AND (%s)`,
		quoteIdent(m.Table),
		strings.Join(setClauses, ", "),
		i,
		strings.Join(nullGuards, " OR "),
	)

	res, err := db.Exec(stmt, vals...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func quoteIdent(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}
