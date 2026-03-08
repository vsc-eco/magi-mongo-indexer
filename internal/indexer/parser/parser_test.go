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
