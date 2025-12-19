// Package indexer provides the core indexing logic and manages global
// state for mappings and views used throughout the indexer service.
package mapper

import (
	"sync"

	"github.com/tibfox/magi-mongo-indexer/internal/indexer/types"
)

// State represents the in-memory global indexer state.
//
// It holds the *latest* mappings (contract → table/event rules)
// and views (SQL definitions) that are currently active.
//
// Access is synchronized via an RWMutex so that multiple goroutines
// (e.g. backfill workers, WebSocket listeners, Hasura sync) can safely
// read from state concurrently while file watchers update it.
type State struct {
	mu       sync.RWMutex
	mappings *types.MappingFile
	views    *types.ViewsFile
}

// globalState is the singleton instance of State that all indexer
// components read from. It is updated whenever mappings.yaml or
// views.yaml are reloaded by the file watcher.
var globalState = &State{}

// GetMappings returns the currently active MappingFile.
//
// The MappingFile defines all contract → event → table mappings
// as loaded from mappings.yaml. If no mappings have been loaded yet,
// it may return nil.
//
// This function acquires a read lock so that concurrent readers
// can safely access the state.
func GetMappings() *types.MappingFile {
	globalState.mu.RLock()
	defer globalState.mu.RUnlock()
	return globalState.mappings
}

// GetViews returns the currently active ViewsFile.
//
// The ViewsFile contains SQL definitions for application-level views
// as loaded from views.yaml. If no views have been loaded yet,
// it may return nil.
//
// This function acquires a read lock so that concurrent readers
// can safely access the state.
func GetViews() *types.ViewsFile {
	globalState.mu.RLock()
	defer globalState.mu.RUnlock()
	return globalState.views
}

// UpdateState replaces the current global mappings and views with
// the provided values.
//
// This is typically called by the file watcher after it reloads
// mappings.yaml and views.yaml from disk. Once updated, all subsequent
// calls to GetMappings and GetViews will return the new values.
//
// This function acquires a write lock to ensure that the update
// is atomic and visible to all concurrent readers.
func UpdateState(m *types.MappingFile, v *types.ViewsFile) {
	globalState.mu.Lock()
	defer globalState.mu.Unlock()
	globalState.mappings = m
	globalState.views = v
}

// ListContracts returns a deduplicated slice of all contract addresses
// defined in the currently active mappings. If no mappings are loaded,
// it returns an empty slice.
//
// This function acquires a read lock to safely access the global state.
func ListContracts() []string {
	mappings := GetMappings()
	if mappings == nil {
		return nil
	}
	out := []string{}
	for _, c := range mappings.Contracts {
		if c.Address != "" {
			out = append(out, c.Address)
		}
	}
	return out
}

// SetMappings replaces only the global mappings (without touching views).
// This is primarily used in unit tests to inject fake mappings quickly.
func SetMappings(m *types.MappingFile) {
	globalState.mu.Lock()
	defer globalState.mu.Unlock()
	globalState.mappings = m
}
