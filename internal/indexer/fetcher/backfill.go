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

	// Expand the mapping set the same way backfill.Run does: a template entry
	// carries no Address, so iterating Contracts alone silently SKIPS every
	// auto-discovered contract. Build addr -> fromBlockHeight covering both.
	targets := map[string]*uint64{}
	for _, contract := range current.Contracts {
		if contract.Address != "" {
			targets[contract.Address] = contract.FromBlockHeight
			continue
		}
		if contract.DiscoverEvent == "" {
			continue
		}
		for _, addr := range mapper.GetDiscoveredContracts(contract.DiscoverEvent) {
			targets[addr] = contract.FromBlockHeight
		}
	}

	for addr, fromBlockHeight := range targets {
		log.Printf("[backfill] Replaying logs for contract %s", addr)

		var rows *sql.Rows
		var err error

		// Respect optional fromBlockHeight
		if fromBlockHeight != nil {
			rows, err = db.Query(`
				SELECT block_height, tx_hash, contract_address, log, ts
				FROM contract_logs
				WHERE contract_address = $1
				  AND block_height >= $2
				ORDER BY block_height ASC`,
				addr, *fromBlockHeight,
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

		// Closed at the end of THIS iteration. A `defer` here would hold every
		// contract's result set open until the whole replay finished.
		scanErr := func() error {
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
			return rows.Err()
		}()
		if scanErr != nil {
			return scanErr
		}
	}

	return nil
}
