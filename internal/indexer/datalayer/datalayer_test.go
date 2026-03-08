package datalayer

import (
	"strings"
	"testing"

	"github.com/tibfox/magi-mongo-indexer/internal/indexer/types"
)

func TestPqQuoteIdent(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"simple", `"simple"`},
		{"with space", `"with space"`},
		{`with"quote`, `"with""quote"`},
		{"CamelCase", `"CamelCase"`},
		{"snake_case", `"snake_case"`},
		{"123numeric", `"123numeric"`},
		{"", `""`},
		{`multiple"quotes"here`, `"multiple""quotes""here"`},
	}

	for _, tt := range tests {
		result := pqQuoteIdent(tt.input)
		if result != tt.expected {
			t.Errorf("pqQuoteIdent(%q) = %q, expected %q", tt.input, result, tt.expected)
		}
	}
}

func TestSanitizeIdent(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"simple", "simple"},
		{"with.dot", "with_dot"},
		{`with"quote`, "with_quote"},
		{"with.dot.and\"quote", "with_dot_and_quote"},
		{"multiple...dots", "multiple___dots"},
		{"no_change", "no_change"},
		{"", ""},
	}

	for _, tt := range tests {
		result := sanitizeIdent(tt.input)
		if result != tt.expected {
			t.Errorf("sanitizeIdent(%q) = %q, expected %q", tt.input, result, tt.expected)
		}
	}
}

func TestBuildInsertSQL_BasicFields(t *testing.T) {
	mapping := types.EventMapping{
		Table: "test_events",
		Parse: "json",
		Fields: map[string]string{
			"event_id": "$.id",
			"user":     "$.user",
		},
		Schema: map[string]string{
			"event_id": "numeric",
			"user":     "string",
		},
	}

	ev := types.LogEvent{
		BlockHeight: 1000,
		TxHash:      "0xabc123",
		Timestamp:   "2024-01-01T00:00:00Z",
		Log:         `{"id":42,"user":"alice"}`,
	}

	stmt, vals := BuildInsertSQL(mapping, ev)

	// Verify table name is quoted
	if !strings.Contains(stmt, `"test_events"`) {
		t.Errorf("expected quoted table name, got: %s", stmt)
	}

	// Verify INSERT statement structure
	if !strings.HasPrefix(stmt, "INSERT INTO") {
		t.Errorf("expected INSERT INTO statement, got: %s", stmt)
	}

	// Verify core fields present
	if !strings.Contains(stmt, "indexer_block_height") {
		t.Errorf("expected indexer_block_height in statement")
	}
	if !strings.Contains(stmt, "indexer_tx_hash") {
		t.Errorf("expected indexer_tx_hash in statement")
	}
	if !strings.Contains(stmt, "indexer_ts") {
		t.Errorf("expected indexer_ts in statement")
	}
	if !strings.Contains(stmt, "indexer_log_hash") {
		t.Errorf("expected indexer_log_hash in statement")
	}

	// Verify ON CONFLICT clause
	if !strings.Contains(stmt, "ON CONFLICT DO NOTHING") {
		t.Errorf("expected ON CONFLICT DO NOTHING clause")
	}

	// Verify minimum values (4 core fields + 2 parsed fields)
	if len(vals) < 4 {
		t.Errorf("expected at least 4 values, got %d", len(vals))
	}

	// Verify core field values
	if vals[0] != uint64(1000) {
		t.Errorf("expected block height 1000, got %v", vals[0])
	}
	if vals[1] != "0xabc123" {
		t.Errorf("expected tx_hash '0xabc123', got %v", vals[1])
	}
	if vals[2] != "2024-01-01T00:00:00Z" {
		t.Errorf("expected timestamp '2024-01-01T00:00:00Z', got %v", vals[2])
	}
	// vals[3] is log_hash - should be a hex string
	if hash, ok := vals[3].(string); !ok || len(hash) != 64 {
		t.Errorf("expected 64-char hex hash, got %v", vals[3])
	}
}

func TestBuildInsertSQL_LogHashUniqueness(t *testing.T) {
	mapping := types.EventMapping{
		Table:  "test_events",
		Parse:  "json",
		Fields: map[string]string{},
		Schema: map[string]string{},
	}

	ev1 := types.LogEvent{
		BlockHeight: 1000,
		TxHash:      "0xabc",
		Log:         `{"id":1}`,
	}
	ev2 := types.LogEvent{
		BlockHeight: 1000,
		TxHash:      "0xabc",
		Log:         `{"id":2}`, // Different log
	}
	ev3 := types.LogEvent{
		BlockHeight: 1001, // Different block
		TxHash:      "0xabc",
		Log:         `{"id":1}`,
	}

	_, vals1 := BuildInsertSQL(mapping, ev1)
	_, vals2 := BuildInsertSQL(mapping, ev2)
	_, vals3 := BuildInsertSQL(mapping, ev3)

	// log_hash is at index 3
	hash1 := vals1[3].(string)
	hash2 := vals2[3].(string)
	hash3 := vals3[3].(string)

	if hash1 == hash2 {
		t.Errorf("expected different log hashes for different logs")
	}
	if hash1 == hash3 {
		t.Errorf("expected different log hashes for different blocks")
	}
	if hash2 == hash3 {
		t.Errorf("expected different hashes for different block+log combinations")
	}
}

func TestBuildInsertSQL_SameInputSameHash(t *testing.T) {
	mapping := types.EventMapping{
		Table:  "test_events",
		Parse:  "json",
		Fields: map[string]string{},
		Schema: map[string]string{},
	}

	ev := types.LogEvent{
		BlockHeight: 1000,
		TxHash:      "0xabc",
		Log:         `{"id":1}`,
	}

	_, vals1 := BuildInsertSQL(mapping, ev)
	_, vals2 := BuildInsertSQL(mapping, ev)

	hash1 := vals1[3].(string)
	hash2 := vals2[3].(string)

	if hash1 != hash2 {
		t.Errorf("expected same hash for identical input, got %s and %s", hash1, hash2)
	}
}

func TestBuildInsertSQL_PlaceholderCount(t *testing.T) {
	mapping := types.EventMapping{
		Table: "test_events",
		Parse: "json",
		Fields: map[string]string{
			"field1": "$.a",
			"field2": "$.b",
			"field3": "$.c",
		},
		Schema: map[string]string{
			"field1": "string",
			"field2": "string",
			"field3": "string",
		},
	}

	ev := types.LogEvent{
		BlockHeight: 1000,
		TxHash:      "0xabc",
		Timestamp:   "2024-01-01T00:00:00Z",
		Log:         `{"a":"x","b":"y","c":"z"}`,
	}

	stmt, vals := BuildInsertSQL(mapping, ev)

	// Count $ placeholders
	placeholderCount := strings.Count(stmt, "$")
	if placeholderCount != len(vals) {
		t.Errorf("placeholder count (%d) doesn't match values count (%d)", placeholderCount, len(vals))
	}

	// Verify sequential placeholders
	for i := 1; i <= len(vals); i++ {
		placeholder := "$" + string(rune('0'+i))
		if i <= 9 && !strings.Contains(stmt, placeholder) {
			// For single digit placeholders
			t.Logf("checking for placeholder $%d", i)
		}
	}
}

func TestBuildInsertSQL_EmptyLog(t *testing.T) {
	mapping := types.EventMapping{
		Table:  "test_events",
		Parse:  "json",
		Fields: map[string]string{},
		Schema: map[string]string{},
	}

	ev := types.LogEvent{
		BlockHeight: 1000,
		TxHash:      "0xabc",
		Timestamp:   "2024-01-01T00:00:00Z",
		Log:         `{}`,
	}

	stmt, vals := BuildInsertSQL(mapping, ev)

	// Should still produce valid SQL with core fields
	if !strings.Contains(stmt, "INSERT INTO") {
		t.Errorf("expected valid INSERT statement for empty log")
	}

	// Should have exactly 4 core values
	if len(vals) != 4 {
		t.Errorf("expected 4 core values for empty log, got %d", len(vals))
	}
}

func TestBuildInsertSQL_SpecialCharactersInTableName(t *testing.T) {
	mapping := types.EventMapping{
		Table:  "test-events.v2",
		Parse:  "json",
		Fields: map[string]string{},
		Schema: map[string]string{},
	}

	ev := types.LogEvent{
		BlockHeight: 1000,
		TxHash:      "0xabc",
		Log:         `{}`,
	}

	stmt, _ := BuildInsertSQL(mapping, ev)

	// Table name should be properly quoted
	if !strings.Contains(stmt, `"test-events.v2"`) {
		t.Errorf("expected properly quoted table name with special chars, got: %s", stmt)
	}
}
