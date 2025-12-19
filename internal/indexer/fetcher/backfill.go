package fetcher

import (
	"database/sql"
	"log"

	"github.com/tibfox/magi-mongo-indexer/internal/indexer/datalayer"
	"github.com/tibfox/magi-mongo-indexer/internal/indexer/mapper"
	"github.com/tibfox/magi-mongo-indexer/internal/indexer/types"
)

// BackfillFromGeneric replays logs stored in the generic `contract_logs` table
// into contract-specific tables using the *latest* mappings.
func BackfillFromGeneric(db *sql.DB) error {
	current := mapper.GetMappings()
	if current == nil {
		log.Println("[backfill] no mappings loaded; skipping replay")
		return nil
	}

	for _, contract := range current.Contracts {
		addr := contract.Address
		if addr == "" {
			continue
		}

		log.Printf("[backfill] Replaying logs for contract %s", addr)

		var rows *sql.Rows
		var err error

		// Respect optional fromBlockHeight
		if contract.FromBlockHeight != nil {
			rows, err = db.Query(`
				SELECT block_height, tx_hash, contract_address, log, ts
				FROM contract_logs
				WHERE contract_address = $1
				  AND block_height >= $2
				ORDER BY block_height ASC`,
				addr, *contract.FromBlockHeight,
			)
		} else {
			rows, err = db.Query(`
				SELECT block_height, tx_hash, contract_address, log, ts
				FROM contract_logs
				WHERE contract_address = $1
				ORDER BY block_height ASC`,
				addr,
			)
		}

		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var ev types.LogEvent
			if err := rows.Scan(&ev.BlockHeight, &ev.TxHash, &ev.ContractAddress, &ev.Log, &ev.Timestamp); err != nil {
				return err
			}

			// Find the correct event mapping under this contract
			if mapping := mapper.FindMapping(ev.ContractAddress, ev.Log); mapping != nil {
				datalayer.InsertLog(db, *mapping, ev)
			} else {
				log.Printf("[backfill] no mapping found for log in contract %s: %s", addr, ev.Log)
			}
		}

		if err := rows.Err(); err != nil {
			return err
		}
	}

	return nil
}
