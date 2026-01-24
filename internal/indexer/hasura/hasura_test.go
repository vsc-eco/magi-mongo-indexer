package hasura

import (
	"testing"

	"github.com/tibfox/magi-mongo-indexer/internal/config"
	"github.com/tibfox/magi-mongo-indexer/internal/indexer/types"
)

func TestGetDesiredAndTracked_DesiredTables(t *testing.T) {
	mappings := &types.MappingFile{
		Contracts: []types.ContractMapping{
			{
				Address: "vsc1test",
				Events: []types.EventMapping{
					{Table: "events_a"},
					{Table: "events_b"},
				},
			},
			{
				Address: "vsc2test",
				Events: []types.EventMapping{
					{Table: "events_c"},
				},
			},
		},
	}

	views := &types.ViewsFile{
		Views: []struct {
			Name string `yaml:"name"`
			SQL  string `yaml:"sql"`
		}{
			{Name: "view_a", SQL: "SELECT 1"},
			{Name: "view_b", SQL: "SELECT 2"},
		},
	}

	// Empty config - no Hasura URL means we skip API calls
	cfg := config.Config{}

	desiredTables, desiredViews, trackedTables, trackedViews, err := GetDesiredAndTracked(mappings, views, cfg)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Check desired tables (should include contract_logs + all mapped tables)
	expectedTables := []string{"contract_logs", "events_a", "events_b", "events_c"}
	for _, table := range expectedTables {
		if _, ok := desiredTables[table]; !ok {
			t.Errorf("expected table %q in desiredTables", table)
		}
	}

	if len(desiredTables) != len(expectedTables) {
		t.Errorf("expected %d desired tables, got %d", len(expectedTables), len(desiredTables))
	}

	// Check desired views (includes built-in indexer_health view)
	expectedViews := []string{"view_a", "view_b", "indexer_health"}
	for _, view := range expectedViews {
		if _, ok := desiredViews[view]; !ok {
			t.Errorf("expected view %q in desiredViews", view)
		}
	}

	if len(desiredViews) != len(expectedViews) {
		t.Errorf("expected %d desired views, got %d", len(expectedViews), len(desiredViews))
	}

	// With no Hasura URL, tracked should be empty
	if len(trackedTables) != 0 {
		t.Errorf("expected 0 tracked tables without Hasura, got %d", len(trackedTables))
	}
	if len(trackedViews) != 0 {
		t.Errorf("expected 0 tracked views without Hasura, got %d", len(trackedViews))
	}
}

func TestGetDesiredAndTracked_EmptyMappings(t *testing.T) {
	mappings := &types.MappingFile{
		Contracts: []types.ContractMapping{},
	}
	views := &types.ViewsFile{
		Views: []struct {
			Name string `yaml:"name"`
			SQL  string `yaml:"sql"`
		}{},
	}

	cfg := config.Config{}

	desiredTables, desiredViews, _, _, err := GetDesiredAndTracked(mappings, views, cfg)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should still have contract_logs
	if _, ok := desiredTables["contract_logs"]; !ok {
		t.Error("expected contract_logs in desiredTables even with empty mappings")
	}

	if len(desiredTables) != 1 {
		t.Errorf("expected exactly 1 desired table (contract_logs), got %d", len(desiredTables))
	}

	// Should still have the built-in indexer_health view
	if len(desiredViews) != 1 {
		t.Errorf("expected 1 desired view (indexer_health), got %d", len(desiredViews))
	}
	if _, ok := desiredViews["indexer_health"]; !ok {
		t.Error("expected indexer_health view in desiredViews")
	}
}

func TestGetDesiredAndTracked_DuplicateTableNames(t *testing.T) {
	mappings := &types.MappingFile{
		Contracts: []types.ContractMapping{
			{
				Address: "vsc1",
				Events: []types.EventMapping{
					{Table: "shared_table"},
					{Table: "shared_table"}, // duplicate
				},
			},
			{
				Address: "vsc2",
				Events: []types.EventMapping{
					{Table: "shared_table"}, // another duplicate
				},
			},
		},
	}
	views := &types.ViewsFile{}

	cfg := config.Config{}

	desiredTables, _, _, _, err := GetDesiredAndTracked(mappings, views, cfg)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Map should deduplicate - should have contract_logs + shared_table
	if len(desiredTables) != 2 {
		t.Errorf("expected 2 unique tables (contract_logs + shared_table), got %d", len(desiredTables))
	}

	if _, ok := desiredTables["shared_table"]; !ok {
		t.Error("expected shared_table in desiredTables")
	}
}

func TestGetDesiredAndTracked_MultipleContracts(t *testing.T) {
	mappings := &types.MappingFile{
		Contracts: []types.ContractMapping{
			{
				Address: "contract1",
				Events: []types.EventMapping{
					{Table: "table1"},
					{Table: "table2"},
				},
			},
			{
				Address: "contract2",
				Events: []types.EventMapping{
					{Table: "table3"},
				},
			},
			{
				Address: "contract3",
				Events: []types.EventMapping{
					{Table: "table4"},
					{Table: "table5"},
				},
			},
		},
	}
	views := &types.ViewsFile{}

	cfg := config.Config{}

	desiredTables, _, _, _, err := GetDesiredAndTracked(mappings, views, cfg)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have contract_logs + 5 tables from events
	if len(desiredTables) != 6 {
		t.Errorf("expected 6 tables, got %d", len(desiredTables))
	}

	for _, table := range []string{"contract_logs", "table1", "table2", "table3", "table4", "table5"} {
		if _, ok := desiredTables[table]; !ok {
			t.Errorf("missing expected table: %s", table)
		}
	}
}

func TestGetDesiredAndTracked_OnlyViews(t *testing.T) {
	mappings := &types.MappingFile{
		Contracts: []types.ContractMapping{},
	}
	views := &types.ViewsFile{
		Views: []struct {
			Name string `yaml:"name"`
			SQL  string `yaml:"sql"`
		}{
			{Name: "view1", SQL: "SELECT 1"},
			{Name: "view2", SQL: "SELECT 2"},
			{Name: "view3", SQL: "SELECT 3"},
		},
	}

	cfg := config.Config{}

	desiredTables, desiredViews, _, _, err := GetDesiredAndTracked(mappings, views, cfg)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should only have contract_logs
	if len(desiredTables) != 1 {
		t.Errorf("expected 1 table (contract_logs), got %d", len(desiredTables))
	}

	// Should have 3 views + indexer_health = 4
	if len(desiredViews) != 4 {
		t.Errorf("expected 4 views (3 + indexer_health), got %d", len(desiredViews))
	}
}
