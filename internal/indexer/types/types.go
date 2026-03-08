package types

// LogEvent matches the GraphQL subscription payload
type LogEvent struct {
	BlockHeight     uint64 `json:"blockHeight"`
	TxHash          string `json:"txHash"`
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
type EventMapping struct {
	LogType      string            `yaml:"log_type"`
	Table        string            `yaml:"table"`
	Schema       map[string]string `yaml:"schema"`
	Parse        string            `yaml:"parse"`
	Delimiter    string            `yaml:"delimiter,omitempty"`
	KeyDelimiter string            `yaml:"key_delimiter,omitempty"`
	Fields       map[string]string `yaml:"fields"`
}

// ViewsFile defines the structure of a *_views.yaml configuration file.
type ViewsFile struct {
	Views []struct {
		Name string `yaml:"name"`
		SQL  string `yaml:"sql"`
	} `yaml:"views"`
}
