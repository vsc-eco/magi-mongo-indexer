package mapper

import (
	"testing"

	"github.com/tibfox/magi-mongo-indexer/internal/indexer/types"
)

// helper to inject test mappings into global state
func setTestMappings(m *types.MappingFile) {
	UpdateState(m, nil)
}

// fakeMappings simulates a minimal MappingFile with the new structure
var fakeMappings = &types.MappingFile{
	Contracts: []types.ContractMapping{
		{
			Address: "vsc1BV7jzektV1eyh4Wyfaet1Xfz1WzDH72hRh",
			Events: []types.EventMapping{
				{
					LogType:      "m",
					Parse:        "csv",
					Delimiter:    "|",
					KeyDelimiter: "=",
					Table:        "okinoko_iarv2_move_events",
					Fields: map[string]string{
						"id":   "1",
						"by":   "2",
						"cell": "3",
					},
					Schema: map[string]string{
						"id":   "numeric",
						"by":   "string",
						"cell": "numeric",
					},
				},
			},
		},
	},
}

func TestFindMapping_CSV(t *testing.T) {
	setTestMappings(fakeMappings) // use the in-memory global
	addr := "vsc1BV7jzektV1eyh4Wyfaet1Xfz1WzDH72hRh"
	logStr := "m|id=0|by=hive:tibfox.vsc|cell=0|ts=1762446759"

	m := FindMapping(addr, logStr)
	if m == nil {
		t.Fatalf("expected mapping for CSV log, got nil")
	}
	if m.LogType != "m" {
		t.Errorf("expected LogType 'm', got '%s'", m.LogType)
	}
}

func TestFindMapping_JSON(t *testing.T) {
	testMappings := &types.MappingFile{
		Contracts: []types.ContractMapping{
			{
				Address: "vsc1BgfucQVHwYBHuK2yMEv4AhYua9rtQ45Uoe",
				Events: []types.EventMapping{
					{
						LogType: "cr",
						Parse:   "json",
						Table:   "okinoko_escrow_created_events",
					},
				},
			},
		},
	}

	setTestMappings(testMappings)

	addr := "vsc1BgfucQVHwYBHuK2yMEv4AhYua9rtQ45Uoe"
	logStr := `{"type":"cr","attributes":{"id":1}}`

	m := FindMapping(addr, logStr)
	if m == nil {
		t.Fatalf("expected mapping for JSON log, got nil")
	}
	if m.LogType != "cr" {
		t.Errorf("expected LogType 'cr', got '%s'", m.LogType)
	}
}
