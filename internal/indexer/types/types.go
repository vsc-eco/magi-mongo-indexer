package types

// LogEvent matches the GraphQL subscription payload
type LogEvent struct {
	BlockHeight     uint64 `json:"blockHeight"`
	TxHash          string `json:"txHash"`
	OutputHash      string `json:"outputHash"`
	ContractAddress string `json:"contractAddress"`
	Log             string `json:"log"`
	Timestamp       string `json:"timestamp"`
}

// MappingFile represents the contents of a single *_mappings.yaml file.
type MappingFile struct {
	Contracts []ContractMapping `yaml:"contracts"`
}

// ContractMapping defines all event mappings for a specific contract.
// If DiscoverEvent is set (and Address is empty), the indexer auto-discovers
// contracts by scanning ALL contract logs for the given event type.
// Discovered contracts are persisted and indexed using the same event mappings.
type ContractMapping struct {
	Address         string         `yaml:"address"`
	DiscoverEvent   string         `yaml:"discoverEvent,omitempty"`
	FromBlockHeight *uint64        `yaml:"fromBlockHeight,omitempty"`
	Events          []EventMapping `yaml:"events"`
}

// EventMapping defines how a specific log/event should be parsed and stored.
//
// For CSV logs, either Fields or Variants must be set. Variants lets the
// same log_type be parsed under different layouts, selected by token count —
// useful when a contract adds a new field at a fixed position and older logs
// still need to be indexed. All variants write into the same Schema; columns
// absent from a given variant stay NULL for logs parsed by that variant.
type EventMapping struct {
	LogType      string            `yaml:"log_type"`
	Table        string            `yaml:"table"`
	Schema       map[string]string `yaml:"schema"`
	Parse        string            `yaml:"parse"`
	Delimiter    string            `yaml:"delimiter,omitempty"`
	KeyDelimiter string            `yaml:"key_delimiter,omitempty"`
	Fields       map[string]string `yaml:"fields,omitempty"`
	Variants     []EventVariant    `yaml:"variants,omitempty"`
}

// EventVariant describes one possible CSV layout for an event. FieldCount is
// the total number of tokens after splitting by Delimiter (including the
// leading log_type token).
type EventVariant struct {
	FieldCount int               `yaml:"field_count"`
	Fields     map[string]string `yaml:"fields"`
}

// ViewsFile defines the structure of a *_views.yaml configuration file.
type ViewsFile struct {
	Views []struct {
		Name string `yaml:"name"`
		SQL  string `yaml:"sql"`
	} `yaml:"views"`
}
