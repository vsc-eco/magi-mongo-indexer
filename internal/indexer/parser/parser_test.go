package parser

import (
	"testing"

	"github.com/tibfox/magi-mongo-indexer/internal/indexer/types"
)

func TestGetNestedValue_SingleLevel(t *testing.T) {
	data := map[string]interface{}{
		"type": "cr",
		"id":   float64(123),
	}

	if v := getNestedValue(data, "type"); v != "cr" {
		t.Errorf("expected 'cr', got %v", v)
	}
	if v := getNestedValue(data, "id"); v != float64(123) {
		t.Errorf("expected 123, got %v", v)
	}
}

func TestGetNestedValue_TwoLevels(t *testing.T) {
	data := map[string]interface{}{
		"attributes": map[string]interface{}{
			"id":   float64(42),
			"from": "alice",
		},
	}

	if v := getNestedValue(data, "attributes.id"); v != float64(42) {
		t.Errorf("expected 42, got %v", v)
	}
	if v := getNestedValue(data, "attributes.from"); v != "alice" {
		t.Errorf("expected 'alice', got %v", v)
	}
}

func TestGetNestedValue_DeepNesting(t *testing.T) {
	data := map[string]interface{}{
		"user": map[string]interface{}{
			"profile": map[string]interface{}{
				"email": map[string]interface{}{
					"verified": true,
					"address":  "test@example.com",
				},
			},
		},
	}

	if v := getNestedValue(data, "user.profile.email.verified"); v != true {
		t.Errorf("expected true, got %v", v)
	}
	if v := getNestedValue(data, "user.profile.email.address"); v != "test@example.com" {
		t.Errorf("expected 'test@example.com', got %v", v)
	}
}

func TestGetNestedValue_MissingPath(t *testing.T) {
	data := map[string]interface{}{
		"type": "cr",
	}

	if v := getNestedValue(data, "missing"); v != nil {
		t.Errorf("expected nil for missing key, got %v", v)
	}
	if v := getNestedValue(data, "deep.missing.path"); v != nil {
		t.Errorf("expected nil for missing nested path, got %v", v)
	}
}

func TestGetNestedValue_NonMapIntermediate(t *testing.T) {
	data := map[string]interface{}{
		"value": "string_not_map",
	}

	if v := getNestedValue(data, "value.nested"); v != nil {
		t.Errorf("expected nil when traversing non-map, got %v", v)
	}
}

func TestParseLog_JSON_SingleLevel(t *testing.T) {
	mapping := types.EventMapping{
		Parse: "json",
		Fields: map[string]string{
			"event_type": "$.type",
			"event_id":   "$.id",
		},
	}
	logStr := `{"type":"cr","id":123}`

	result := ParseLog(mapping, logStr)

	if result["event_type"] != "cr" {
		t.Errorf("expected event_type 'cr', got '%v'", result["event_type"])
	}
	if result["event_id"] != float64(123) {
		t.Errorf("expected event_id 123, got '%v'", result["event_id"])
	}
}

func TestParseLog_JSON_NestedTwoLevels(t *testing.T) {
	mapping := types.EventMapping{
		Parse: "json",
		Fields: map[string]string{
			"id":   "$.attributes.id",
			"from": "$.attributes.from",
		},
	}
	logStr := `{"type":"cr","attributes":{"id":42,"from":"alice"}}`

	result := ParseLog(mapping, logStr)

	if result["id"] != float64(42) {
		t.Errorf("expected id 42, got '%v'", result["id"])
	}
	if result["from"] != "alice" {
		t.Errorf("expected from 'alice', got '%v'", result["from"])
	}
}

func TestParseLog_JSON_DeepNesting(t *testing.T) {
	mapping := types.EventMapping{
		Parse: "json",
		Fields: map[string]string{
			"verified": "$.user.profile.email.verified",
			"address":  "$.user.profile.email.address",
		},
	}
	logStr := `{"user":{"profile":{"email":{"verified":true,"address":"test@example.com"}}}}`

	result := ParseLog(mapping, logStr)

	if result["verified"] != true {
		t.Errorf("expected verified true, got '%v'", result["verified"])
	}
	if result["address"] != "test@example.com" {
		t.Errorf("expected address 'test@example.com', got '%v'", result["address"])
	}
}

func TestParseLog_JSON_MissingPath(t *testing.T) {
	mapping := types.EventMapping{
		Parse: "json",
		Fields: map[string]string{
			"missing": "$.does.not.exist",
		},
	}
	logStr := `{"type":"cr"}`

	result := ParseLog(mapping, logStr)

	if _, ok := result["missing"]; ok {
		t.Errorf("expected missing field to be absent, got '%v'", result["missing"])
	}
}

func TestParseLog_JSON_InvalidJSON(t *testing.T) {
	mapping := types.EventMapping{
		Parse: "json",
		Fields: map[string]string{
			"id": "$.id",
		},
	}
	logStr := `not valid json`

	result := ParseLog(mapping, logStr)

	if len(result) != 0 {
		t.Errorf("expected empty result for invalid JSON, got %v", result)
	}
}

func TestParseLog_CSV_BasicParsing(t *testing.T) {
	mapping := types.EventMapping{
		Parse:     "csv",
		Delimiter: "|",
		Fields: map[string]string{
			"type": "0",
			"id":   "1",
			"by":   "2",
		},
		Schema: map[string]string{
			"type": "string",
			"id":   "numeric",
			"by":   "string",
		},
	}
	logStr := "m|42|alice"

	result := ParseLog(mapping, logStr)

	if result["type"] != "m" {
		t.Errorf("expected type 'm', got '%v'", result["type"])
	}
	if result["id"] != float64(42) {
		t.Errorf("expected id 42, got '%v'", result["id"])
	}
	if result["by"] != "alice" {
		t.Errorf("expected by 'alice', got '%v'", result["by"])
	}
}

func TestParseLog_CSV_WithKeyDelimiter(t *testing.T) {
	mapping := types.EventMapping{
		Parse:        "csv",
		Delimiter:    "|",
		KeyDelimiter: "=",
		Fields: map[string]string{
			"id":   "1",
			"cell": "2",
		},
		Schema: map[string]string{
			"id":   "numeric",
			"cell": "numeric",
		},
	}
	logStr := "m|id=100|cell=5"

	result := ParseLog(mapping, logStr)

	if result["id"] != float64(100) {
		t.Errorf("expected id 100, got '%v'", result["id"])
	}
	if result["cell"] != float64(5) {
		t.Errorf("expected cell 5, got '%v'", result["cell"])
	}
}

func TestParseLog_CSV_BooleanType(t *testing.T) {
	mapping := types.EventMapping{
		Parse:     "csv",
		Delimiter: ",",
		Fields: map[string]string{
			"active": "0",
		},
		Schema: map[string]string{
			"active": "bool",
		},
	}

	result := ParseLog(mapping, "true")
	if result["active"] != true {
		t.Errorf("expected active true, got '%v'", result["active"])
	}

	result = ParseLog(mapping, "false")
	if result["active"] != false {
		t.Errorf("expected active false, got '%v'", result["active"])
	}
}

func TestParseLog_CSV_WhitespaceDelimiter(t *testing.T) {
	mapping := types.EventMapping{
		Parse:     "csv",
		Delimiter: "", // empty means whitespace
		Fields: map[string]string{
			"type": "0",
			"id":   "1",
		},
		Schema: map[string]string{
			"type": "string",
			"id":   "numeric",
		},
	}
	logStr := "event   123" // multiple spaces

	result := ParseLog(mapping, logStr)

	if result["type"] != "event" {
		t.Errorf("expected type 'event', got '%v'", result["type"])
	}
	if result["id"] != float64(123) {
		t.Errorf("expected id 123, got '%v'", result["id"])
	}
}

func TestParseLog_CSV_OutOfBoundsIndex(t *testing.T) {
	mapping := types.EventMapping{
		Parse:     "csv",
		Delimiter: "|",
		Fields: map[string]string{
			"missing": "10", // index out of bounds
		},
		Schema: map[string]string{
			"missing": "string",
		},
	}
	logStr := "a|b|c"

	result := ParseLog(mapping, logStr)

	if _, ok := result["missing"]; ok {
		t.Errorf("expected missing field for out of bounds index, got '%v'", result["missing"])
	}
}

func TestParseLog_CSV_NumericParseError(t *testing.T) {
	mapping := types.EventMapping{
		Parse:     "csv",
		Delimiter: ",",
		Fields: map[string]string{
			"num": "0",
		},
		Schema: map[string]string{
			"num": "numeric",
		},
	}
	logStr := "not_a_number"

	result := ParseLog(mapping, logStr)

	// Should default to 0 on parse error
	if result["num"] != float64(0) {
		t.Errorf("expected num 0 on parse error, got '%v'", result["num"])
	}
}

func TestParseLog_CSV_Variants_NewFormatMatches(t *testing.T) {
	// Simulates the DEX fee event gaining an `asset` field at position 1:
	//   old: fee|t=108|m=27|lp=81        (4 parts)
	//   new: fee|a=hive|t=108|m=27|lp=81 (5 parts)
	mapping := types.EventMapping{
		Parse:        "csv",
		Delimiter:    "|",
		KeyDelimiter: "=",
		Schema: map[string]string{
			"asset":     "string",
			"total_fee": "numeric",
			"magi_fee":  "numeric",
			"lp_fee":    "numeric",
		},
		Variants: []types.EventVariant{
			{
				FieldCount: 4,
				Fields:     map[string]string{"total_fee": "1", "magi_fee": "2", "lp_fee": "3"},
			},
			{
				FieldCount: 5,
				Fields:     map[string]string{"asset": "1", "total_fee": "2", "magi_fee": "3", "lp_fee": "4"},
			},
		},
	}

	result := ParseLog(mapping, "fee|a=hive|t=108|m=27|lp=81")

	if result["asset"] != "hive" {
		t.Errorf("expected asset 'hive', got '%v'", result["asset"])
	}
	if result["total_fee"] != float64(108) {
		t.Errorf("expected total_fee 108, got '%v'", result["total_fee"])
	}
	if result["magi_fee"] != float64(27) {
		t.Errorf("expected magi_fee 27, got '%v'", result["magi_fee"])
	}
	if result["lp_fee"] != float64(81) {
		t.Errorf("expected lp_fee 81, got '%v'", result["lp_fee"])
	}
}

func TestParseLog_CSV_Variants_LegacyFormatMatches(t *testing.T) {
	mapping := types.EventMapping{
		Parse:        "csv",
		Delimiter:    "|",
		KeyDelimiter: "=",
		Schema: map[string]string{
			"asset":     "string",
			"total_fee": "numeric",
			"magi_fee":  "numeric",
			"lp_fee":    "numeric",
		},
		Variants: []types.EventVariant{
			{
				FieldCount: 4,
				Fields:     map[string]string{"total_fee": "1", "magi_fee": "2", "lp_fee": "3"},
			},
			{
				FieldCount: 5,
				Fields:     map[string]string{"asset": "1", "total_fee": "2", "magi_fee": "3", "lp_fee": "4"},
			},
		},
	}

	result := ParseLog(mapping, "fee|t=33|m=8|lp=25")

	if _, ok := result["asset"]; ok {
		t.Errorf("expected asset to be absent for legacy variant, got '%v'", result["asset"])
	}
	if result["total_fee"] != float64(33) {
		t.Errorf("expected total_fee 33, got '%v'", result["total_fee"])
	}
	if result["magi_fee"] != float64(8) {
		t.Errorf("expected magi_fee 8, got '%v'", result["magi_fee"])
	}
	if result["lp_fee"] != float64(25) {
		t.Errorf("expected lp_fee 25, got '%v'", result["lp_fee"])
	}
}

func TestParseLog_CSV_Variants_PendulumFormatMatches(t *testing.T) {
	// The pendulum DEX fee log carries the full output-side fee breakdown:
	//   fee|a=hive|lp=81|ns=20|nc=7|nb=27|m=10000|s=9800   (8 parts)
	// total_fee/magi_fee are not emitted; the dex_pool views derive them.
	mapping := types.EventMapping{
		Parse:        "csv",
		Delimiter:    "|",
		KeyDelimiter: "=",
		Schema: map[string]string{
			"total_fee":       "numeric",
			"magi_fee":        "numeric",
			"lp_fee":          "numeric",
			"asset":           "string",
			"node_share":      "numeric",
			"network_credit":  "numeric",
			"node_bucket_hbd": "numeric",
			"multiplier_bps":  "numeric",
			"s_bps":           "numeric",
		},
		Variants: []types.EventVariant{
			{FieldCount: 4, Fields: map[string]string{"total_fee": "1", "magi_fee": "2", "lp_fee": "3"}},
			{FieldCount: 5, Fields: map[string]string{"asset": "1", "total_fee": "2", "magi_fee": "3", "lp_fee": "4"}},
			{FieldCount: 8, Fields: map[string]string{"asset": "1", "lp_fee": "2", "node_share": "3", "network_credit": "4", "node_bucket_hbd": "5", "multiplier_bps": "6", "s_bps": "7"}},
		},
	}

	result := ParseLog(mapping, "fee|a=hive|lp=81|ns=20|nc=7|nb=27|m=10000|s=9800")

	if result["asset"] != "hive" {
		t.Errorf("expected asset hive, got '%v'", result["asset"])
	}
	if result["lp_fee"] != float64(81) {
		t.Errorf("expected lp_fee 81, got '%v'", result["lp_fee"])
	}
	if result["node_share"] != float64(20) {
		t.Errorf("expected node_share 20, got '%v'", result["node_share"])
	}
	if result["network_credit"] != float64(7) {
		t.Errorf("expected network_credit 7, got '%v'", result["network_credit"])
	}
	if result["node_bucket_hbd"] != float64(27) {
		t.Errorf("expected node_bucket_hbd 27, got '%v'", result["node_bucket_hbd"])
	}
	if result["multiplier_bps"] != float64(10000) {
		t.Errorf("expected multiplier_bps 10000, got '%v'", result["multiplier_bps"])
	}
	if result["s_bps"] != float64(9800) {
		t.Errorf("expected s_bps 9800, got '%v'", result["s_bps"])
	}
	// total_fee/magi_fee are not present in the pendulum layout.
	if _, ok := result["total_fee"]; ok {
		t.Errorf("expected total_fee absent for pendulum variant, got '%v'", result["total_fee"])
	}
	if _, ok := result["magi_fee"]; ok {
		t.Errorf("expected magi_fee absent for pendulum variant, got '%v'", result["magi_fee"])
	}
}

func TestParseLog_CSV_Variants_NoMatchReturnsEmpty(t *testing.T) {
	// A log whose part count matches no variant should be skipped (empty result).
	mapping := types.EventMapping{
		Parse:     "csv",
		Delimiter: "|",
		Schema: map[string]string{
			"a": "string",
		},
		Variants: []types.EventVariant{
			{FieldCount: 4, Fields: map[string]string{"a": "1"}},
			{FieldCount: 5, Fields: map[string]string{"a": "2"}},
		},
	}

	result := ParseLog(mapping, "fee|x|y") // 3 parts — matches neither

	if len(result) != 0 {
		t.Errorf("expected empty result for unmatched variant, got %v", result)
	}
}

func TestParseLog_CSV_FloatParsing(t *testing.T) {
	mapping := types.EventMapping{
		Parse:     "csv",
		Delimiter: ",",
		Fields: map[string]string{
			"amount": "0",
		},
		Schema: map[string]string{
			"amount": "numeric",
		},
	}
	logStr := "123.456"

	result := ParseLog(mapping, logStr)

	if result["amount"] != float64(123.456) {
		t.Errorf("expected amount 123.456, got '%v'", result["amount"])
	}
}
