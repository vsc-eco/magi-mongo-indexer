package mapper

import (
	"database/sql"
	"log"
	"sync"
)

// DiscoveryRegistry tracks contracts discovered at runtime via DiscoverEvent scanning.
// Maps discoverEvent → set of contract addresses.
type DiscoveryRegistry struct {
	mu        sync.RWMutex
	contracts map[string]map[string]struct{} // discoverEvent → {contractID: {}}
}

var discoveryRegistry = &DiscoveryRegistry{
	contracts: make(map[string]map[string]struct{}),
}

// EnsureDiscoveredContractsTable creates the persistence table for discovered contracts.
func EnsureDiscoveredContractsTable(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS discovered_contracts (
			id SERIAL PRIMARY KEY,
			contract_id TEXT UNIQUE NOT NULL,
			discover_event TEXT NOT NULL,
			block_height BIGINT,
			discovered_at TIMESTAMP DEFAULT NOW()
		)
	`)
	return err
}

// LoadDiscoveredContracts loads all previously discovered contracts from the DB into memory.
func LoadDiscoveredContracts(db *sql.DB) error {
	rows, err := db.Query(`SELECT contract_id, discover_event FROM discovered_contracts`)
	if err != nil {
		return err
	}
	defer rows.Close()

	discoveryRegistry.mu.Lock()
	defer discoveryRegistry.mu.Unlock()

	for rows.Next() {
		var contractID, discoverEvent string
		if err := rows.Scan(&contractID, &discoverEvent); err != nil {
			log.Printf("[discovery] failed to scan row: %v", err)
			continue
		}
		if discoveryRegistry.contracts[discoverEvent] == nil {
			discoveryRegistry.contracts[discoverEvent] = make(map[string]struct{})
		}
		discoveryRegistry.contracts[discoverEvent][contractID] = struct{}{}
	}

	// Log summary
	for event, contracts := range discoveryRegistry.contracts {
		log.Printf("[discovery] loaded %d discovered contracts for event %q", len(contracts), event)
	}
	return rows.Err()
}

// RegisterDiscoveredContract persists a newly discovered contract and adds it to the in-memory registry.
// Returns true if the contract was newly registered, false if already known.
func RegisterDiscoveredContract(db *sql.DB, contractID, discoverEvent string, blockHeight uint64) (bool, error) {
	// Check if already known
	discoveryRegistry.mu.RLock()
	if set, ok := discoveryRegistry.contracts[discoverEvent]; ok {
		if _, exists := set[contractID]; exists {
			discoveryRegistry.mu.RUnlock()
			return false, nil
		}
	}
	discoveryRegistry.mu.RUnlock()

	// Persist to DB
	_, err := db.Exec(
		`INSERT INTO discovered_contracts (contract_id, discover_event, block_height)
		 VALUES ($1, $2, $3) ON CONFLICT (contract_id) DO NOTHING`,
		contractID, discoverEvent, blockHeight,
	)
	if err != nil {
		return false, err
	}

	// Add to in-memory registry
	discoveryRegistry.mu.Lock()
	defer discoveryRegistry.mu.Unlock()
	if discoveryRegistry.contracts[discoverEvent] == nil {
		discoveryRegistry.contracts[discoverEvent] = make(map[string]struct{})
	}
	discoveryRegistry.contracts[discoverEvent][contractID] = struct{}{}

	log.Printf("[discovery] registered new contract %s via event %q at block %d", contractID, discoverEvent, blockHeight)
	return true, nil
}

// GetDiscoveredContracts returns all contract addresses discovered via a given event type.
func GetDiscoveredContracts(discoverEvent string) []string {
	discoveryRegistry.mu.RLock()
	defer discoveryRegistry.mu.RUnlock()

	set := discoveryRegistry.contracts[discoverEvent]
	if len(set) == 0 {
		return nil
	}
	out := make([]string, 0, len(set))
	for addr := range set {
		out = append(out, addr)
	}
	return out
}

// IsDiscoveredContract checks if a contract was discovered via any event.
// Returns the discoverEvent and true if found.
func IsDiscoveredContract(contractID string) (string, bool) {
	discoveryRegistry.mu.RLock()
	defer discoveryRegistry.mu.RUnlock()

	for event, set := range discoveryRegistry.contracts {
		if _, ok := set[contractID]; ok {
			return event, true
		}
	}
	return "", false
}
