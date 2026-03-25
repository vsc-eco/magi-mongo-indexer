package datalayer

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"strings"

	"github.com/tibfox/magi-mongo-indexer/internal/indexer/parser"
	"github.com/tibfox/magi-mongo-indexer/internal/indexer/types"
)

// EnsureTables creates all required database tables.
//
// It creates:
//  1. A generic `contract_logs` table for storing all raw logs.
//  2. A contract-specific table for each mapping defined in MappingFile.Contracts.Events.
//     Each contract table has schema defined by the mapping (string/numeric/bool).
//
// This allows both a canonical store of all logs and structured, query-friendly
// tables per contract.
func EnsureTables(db *sql.DB, mappings *types.MappingFile) error {
	// Create generic log storage table
	_, err := db.Exec(`
	CREATE TABLE IF NOT EXISTS contract_logs (
		id SERIAL PRIMARY KEY,
		block_height BIGINT,
		tx_hash TEXT,
		contract_address TEXT,
		log TEXT,
		log_hash TEXT GENERATED ALWAYS AS (md5(log)) STORED,
		ts TIMESTAMP,
		UNIQUE (block_height, tx_hash, contract_address, log_hash)
	)`)
	if err != nil {
		return err
	}

	// Create contract-specific tables based on schema in mapping
	for _, contract := range mappings.Contracts {
		for _, m := range contract.Events {
			cols := []string{
				"indexer_id SERIAL PRIMARY KEY",
				"indexer_block_height BIGINT",
				"indexer_tx_hash TEXT",
				"indexer_ts TIMESTAMP",
				"indexer_log_hash TEXT",
				"indexer_contract_id TEXT",
			}

			// Add user-defined schema columns
			for col, typ := range m.Schema {
				switch typ {
				case "string":
					cols = append(cols, fmt.Sprintf("%s TEXT", pqQuoteIdent(col)))
				case "numeric":
					cols = append(cols, fmt.Sprintf("%s NUMERIC", pqQuoteIdent(col)))
				case "bool", "boolean":
					cols = append(cols, fmt.Sprintf("%s BOOLEAN", pqQuoteIdent(col)))
				}
			}

			// Build CREATE TABLE statement with unique constraint on block/tx
			stmt := fmt.Sprintf(
				`CREATE TABLE IF NOT EXISTS %s (%s)`,
				pqQuoteIdent(m.Table), strings.Join(cols, ", "),
			)
			if _, err := db.Exec(stmt); err != nil {
				return err
			}

			// Ensure all mapped columns exist on already-created tables.
			for col, typ := range m.Schema {
				var sqlType string
				switch typ {
				case "string":
					sqlType = "TEXT"
				case "numeric":
					sqlType = "NUMERIC"
				case "bool", "boolean":
					sqlType = "BOOLEAN"
				}
				if sqlType == "" {
					continue
				}
				if _, err := db.Exec(fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s`, pqQuoteIdent(m.Table), pqQuoteIdent(col), sqlType)); err != nil {
					return err
				}
			}

			if _, err := db.Exec(fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS indexer_log_hash TEXT`, pqQuoteIdent(m.Table))); err != nil {
				return err
			}

			if _, err := db.Exec(fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS indexer_contract_id TEXT`, pqQuoteIdent(m.Table))); err != nil {
				return err
			}

			idxName := fmt.Sprintf("%s_log_hash_idx", sanitizeIdent(m.Table))
			if _, err := db.Exec(fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS %s ON %s (indexer_log_hash)`, pqQuoteIdent(idxName), pqQuoteIdent(m.Table))); err != nil {
				return err
			}

			log.Println("Ensured table:", m.Table)
		}
	}
	return nil
}

func sanitizeIdent(name string) string {
	replacer := strings.NewReplacer("\"", "_", ".", "_")
	return replacer.Replace(name)
}

// EnsureViews creates or updates all SQL views defined in views.yaml.
func EnsureViews(db *sql.DB, views *types.ViewsFile) error {
	for _, v := range views.Views {
		_, err := db.Exec(v.SQL)
		if err != nil {
			return fmt.Errorf("failed to create/update view %s: %w", v.Name, err)
		}
		log.Printf("[views] created/updated view: %s", v.Name)
	}
	return nil
}

// PruneTables drops tables in Postgres that are not in mappings.yaml.
// Skips contract_logs and any table starting with cst_.
func PruneTables(db *sql.DB, mappings *types.MappingFile) error {
	// Collect desired tables
	desired := map[string]struct{}{"contract_logs": {}, "discovered_contracts": {}}
	for _, contract := range mappings.Contracts {
		for _, m := range contract.Events {
			desired[m.Table] = struct{}{}
		}
	}

	rows, err := db.Query(`
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = 'public'
		  AND table_type = 'BASE TABLE'`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return err
		}
		if _, ok := desired[name]; !ok && !strings.HasPrefix(name, "cst_") {
			log.Printf("[prune] dropping table %s", name)
			if _, err := db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s CASCADE`, pqQuoteIdent(name))); err != nil {
				log.Printf("[prune] failed to drop table %s: %v", name, err)
			}
		}
	}
	return nil
}

// PruneViews drops views in Postgres that are not in views.yaml.
// Skips any view starting with cst_.
func PruneViews(db *sql.DB, views *types.ViewsFile) error {
	desired := make(map[string]struct{})
	for _, v := range views.Views {
		desired[v.Name] = struct{}{}
	}

	rows, err := db.Query(`
		SELECT table_name
		FROM information_schema.views
		WHERE table_schema = 'public'`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return err
		}
		if _, ok := desired[name]; !ok && !strings.HasPrefix(name, "cst_") {
			log.Printf("[prune] dropping view %s", name)
			if _, err := db.Exec(fmt.Sprintf(`DROP VIEW IF EXISTS %s CASCADE`, pqQuoteIdent(name))); err != nil {
				log.Printf("[prune] failed to drop view %s: %v", name, err)
			}
		}
	}
	return nil
}

// GetLastIndexedBlock returns the highest block number already stored
// in the generic `contract_logs` table for a given contract.
// Returns -1 if no blocks are found or on error.
func GetLastIndexedBlock(db *sql.DB, contract string) int64 {
	var height sql.NullInt64
	err := db.QueryRow(
		`SELECT MAX(block_height) FROM contract_logs WHERE contract_address = $1`,
		contract,
	).Scan(&height)
	if err != nil {
		log.Println("GetLastIndexedBlock error:", err)
		return -1
	}
	if height.Valid {
		return height.Int64
	}
	return -1
}

// BuildInsertSQL builds the INSERT statement and values without executing.
// Deduplication is enforced with `ON CONFLICT DO NOTHING` on (block_height, tx_hash).
func BuildInsertSQL(mapping types.EventMapping, ev types.LogEvent) (string, []interface{}) {
	parsed := parser.ParseLog(mapping, ev.Log)

	// Core fields
	hashInput := fmt.Sprintf("%d:%s:%s", ev.BlockHeight, ev.TxHash, ev.Log)
	hashBytes := sha256.Sum256([]byte(hashInput))
	logHash := hex.EncodeToString(hashBytes[:])

	cols := []string{"indexer_block_height", "indexer_tx_hash", "indexer_ts", "indexer_log_hash", "indexer_contract_id"}
	vals := []interface{}{ev.BlockHeight, ev.TxHash, ev.Timestamp, logHash, ev.ContractAddress}
	placeholders := []string{"$1", "$2", "$3", "$4", "$5"}
	i := 6

	// Add dynamic columns extracted from the log
	for col, val := range parsed {
		cols = append(cols, pqQuoteIdent(col))
		vals = append(vals, val)
		placeholders = append(placeholders, fmt.Sprintf("$%d", i))
		i++
	}

	// Construct final SQL
	stmt := fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES (%s) ON CONFLICT DO NOTHING`,
		pqQuoteIdent(mapping.Table),
		strings.Join(cols, ","),
		strings.Join(placeholders, ","),
	)

	return stmt, vals
}

// InsertLog builds and executes the INSERT against the DB.
func InsertLog(db *sql.DB, mapping types.EventMapping, ev types.LogEvent) {
	stmt, vals := BuildInsertSQL(mapping, ev)

	if _, err := db.Exec(stmt, vals...); err != nil {
		log.Printf("insert failed: %v", err)
	}
}

// pqQuoteIdent safely quotes PostgreSQL identifiers (table/column names).
func pqQuoteIdent(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}

// EnsureHealthView creates the indexer_health view for monitoring via Hasura.
// This view provides the latest block height and last update time from contract_logs.
func EnsureHealthView(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE OR REPLACE VIEW indexer_health AS
		SELECT
			COALESCE(MAX(block_height), 0) AS latest_block_height,
			COALESCE(MAX(ts), NOW()) AS last_update,
			COUNT(DISTINCT contract_address) AS tracked_contracts,
			COUNT(*) AS total_logs
		FROM contract_logs
	`)
	if err != nil {
		return fmt.Errorf("failed to create indexer_health view: %w", err)
	}
	log.Println("[health] created/updated indexer_health view")
	return nil
}
