package parser

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/tibfox/magi-mongo-indexer/internal/indexer/types"
)

// getNestedValue traverses a map using a dot-separated path and returns the value
// at that path, or nil if not found. Supports arbitrary nesting depth.
// Example: getNestedValue(data, "user.profile.email") returns data["user"]["profile"]["email"]
func getNestedValue(data map[string]interface{}, path string) interface{} {
	parts := strings.Split(path, ".")
	var current interface{} = data

	for _, part := range parts {
		switch v := current.(type) {
		case map[string]interface{}:
			current = v[part]
			if current == nil {
				return nil
			}
		default:
			return nil
		}
	}
	return current
}

// ParseLog extracts values from a log string based on the given mapping rules.
// It supports two parsing strategies depending on mapping.Parse ("json" or "csv").
// Returns a map where keys are column names defined in the mapping, and values
// are the extracted and type-converted values from the log.
func ParseLog(mapping types.EventMapping, logStr string) map[string]interface{} {
	out := make(map[string]interface{})

	switch mapping.Parse {

	case "json":
		// Parse log string as JSON
		var raw map[string]interface{}
		if err := json.Unmarshal([]byte(logStr), &raw); err == nil {
			for col, path := range mapping.Fields {
				key := strings.TrimPrefix(path, "$.")
				if v := getNestedValue(raw, key); v != nil {
					out[col] = v
				}
			}
		}

	case "csv":
		// Parse log string as delimited tokens
		delimiter := mapping.Delimiter
		var parts []string
		if delimiter == "" {
			parts = strings.Fields(logStr) // default whitespace
		} else {
			parts = strings.Split(logStr, delimiter)
		}

		keyDelim := mapping.KeyDelimiter

		for col, idx := range mapping.Fields {
			var pos int
			if _, err := fmt.Sscanf(idx, "%d", &pos); err != nil {
				continue
			}
			if pos >= len(parts) {
				continue
			}

			token := strings.TrimSpace(parts[pos])

			if keyDelim != "" {
				if eq := strings.Index(token, keyDelim); eq != -1 {
					token = token[eq+len(keyDelim):]
				}
			}

			// 🔹 Type conversion based on schema
			switch mapping.Schema[col] {
			case "numeric":
				// Always normalize to float64
				if i, err := strconv.ParseInt(token, 10, 64); err == nil {
					out[col] = float64(i)
				} else if f, err := strconv.ParseFloat(token, 64); err == nil {
					out[col] = f
				} else {
					out[col] = float64(0)
				}
			case "bool":
				if b, err := strconv.ParseBool(token); err == nil {
					out[col] = b
				} else {
					out[col] = false
				}
			default: // string
				out[col] = token
			}
		}
	}

	return out
}
