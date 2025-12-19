package config

import (
	"os"
	"strconv"
	"time"
)

var Global Config

func Init() {
	Global = LoadConfig()
}

// Config holds all runtime configuration values for the indexer service.
// Values are primarily loaded from environment variables, with sensible defaults.
type Config struct {
	DBURL         string        // Postgres connection string
	MongoURI      string        // MongoDB connection URI
	MongoDBName   string        // MongoDB database name
	PollInterval  time.Duration // MongoDB polling interval
	MappingsPath  string        // Path to mappings files (directory or single YAML)
	ViewsPath     string        // Path to views files (directory or single YAML)
	HasuraURL     string        // Hasura GraphQL endpoint (for metadata sync)
	HasuraSecret  string        // Hasura admin secret
	HasuraSource  string        // Hasura source database
}

// LoadConfig reads configuration values from environment variables.
// If a variable is missing, it falls back to a default suitable for local dev.
//
// Environment variables:
//   - DATABASE_URL       → Postgres DSN
//   - MONGO_URI          → MongoDB connection URI
//   - MONGO_DB_NAME      → MongoDB database name
//   - POLL_INTERVAL_SEC  → Polling interval in seconds (default: 5)
//   - MAPPINGS_FILES     → Path to mappings.yaml
//   - VIEWS_FILES        → Path to views.yaml
//   - HASURA_URL         → Hasura endpoint
//   - HASURA_ADMIN_SECRET→ Hasura admin secret
//   - HASURA_SOURCE      → Hasura source database name
//
// Defaults:
//   - DBURL: postgres://indexer:indexerpass@localhost:5432/indexerdb?sslmode=disable
//   - MongoURI: mongodb://localhost:27017
//   - MongoDBName: go-vsc
//   - PollInterval: 5 seconds
//   - MappingsPath: internal/config/events
func LoadConfig() Config {
	pollIntervalSec := 5
	if val := os.Getenv("POLL_INTERVAL_SEC"); val != "" {
		if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 {
			pollIntervalSec = parsed
		}
	}

	cfg := Config{
		DBURL:        os.Getenv("DATABASE_URL"),
		MongoURI:     os.Getenv("MONGO_URI"),
		MongoDBName:  os.Getenv("MONGO_DB_NAME"),
		PollInterval: time.Duration(pollIntervalSec) * time.Second,
		MappingsPath: os.Getenv("MAPPINGS_FILES"),
		ViewsPath:    os.Getenv("VIEWS_FILES"),
		HasuraURL:    os.Getenv("HASURA_URL"),
		HasuraSecret: os.Getenv("HASURA_ADMIN_SECRET"),
		HasuraSource: os.Getenv("HASURA_SOURCE"),
	}

	// Apply defaults if not provided
	if cfg.DBURL == "" {
		cfg.DBURL = "postgres://indexer:indexerpass@localhost:5432/indexerdb?sslmode=disable"
	}
	if cfg.MongoURI == "" {
		cfg.MongoURI = "mongodb://mongo_vsc:27017"
	}
	if cfg.MongoDBName == "" {
		cfg.MongoDBName = "go-vsc"
	}
	if cfg.MappingsPath == "" {
		cfg.MappingsPath = "internal/config/events"
	}
	if cfg.ViewsPath == "" {
		cfg.ViewsPath = "internal/config/events"
	}
	if cfg.HasuraSource == "" {
		cfg.HasuraSource = "default"
	}
	return cfg
}
