package config

import (
	"os"
	"testing"
	"time"
)

func clearEnvVars() {
	envVars := []string{
		"DATABASE_URL", "MONGO_URI", "MONGO_DB_NAME",
		"POLL_INTERVAL_SEC", "MAPPINGS_FILES", "VIEWS_FILES",
		"HASURA_URL", "HASURA_ADMIN_SECRET", "HASURA_SOURCE",
	}
	for _, v := range envVars {
		os.Unsetenv(v)
	}
}

func TestLoadConfig_Defaults(t *testing.T) {
	clearEnvVars()

	cfg := LoadConfig()

	// Verify defaults
	if cfg.DBURL != "postgres://indexer:indexerpass@localhost:5432/indexerdb?sslmode=disable" {
		t.Errorf("unexpected default DBURL: %s", cfg.DBURL)
	}
	if cfg.MongoURI != "mongodb://mongo_vsc:27017" {
		t.Errorf("unexpected default MongoURI: %s", cfg.MongoURI)
	}
	if cfg.MongoDBName != "go-vsc" {
		t.Errorf("unexpected default MongoDBName: %s", cfg.MongoDBName)
	}
	if cfg.PollInterval != 5*time.Second {
		t.Errorf("unexpected default PollInterval: %v", cfg.PollInterval)
	}
	if cfg.MappingsPath != "internal/config/events" {
		t.Errorf("unexpected default MappingsPath: %s", cfg.MappingsPath)
	}
	if cfg.ViewsPath != "internal/config/events" {
		t.Errorf("unexpected default ViewsPath: %s", cfg.ViewsPath)
	}
	if cfg.HasuraSource != "default" {
		t.Errorf("unexpected default HasuraSource: %s", cfg.HasuraSource)
	}
	// HasuraURL and HasuraSecret should be empty by default
	if cfg.HasuraURL != "" {
		t.Errorf("expected empty HasuraURL, got: %s", cfg.HasuraURL)
	}
	if cfg.HasuraSecret != "" {
		t.Errorf("expected empty HasuraSecret, got non-empty value")
	}
}

func TestLoadConfig_EnvOverrides(t *testing.T) {
	clearEnvVars()

	// Set custom values
	os.Setenv("DATABASE_URL", "postgres://custom:5432/db")
	os.Setenv("MONGO_URI", "mongodb://custom:27017")
	os.Setenv("MONGO_DB_NAME", "custom-db")
	os.Setenv("POLL_INTERVAL_SEC", "10")
	os.Setenv("MAPPINGS_FILES", "/custom/mappings")
	os.Setenv("VIEWS_FILES", "/custom/views")
	os.Setenv("HASURA_URL", "http://hasura:8080")
	os.Setenv("HASURA_ADMIN_SECRET", "secret123")
	os.Setenv("HASURA_SOURCE", "custom_source")

	defer clearEnvVars()

	cfg := LoadConfig()

	if cfg.DBURL != "postgres://custom:5432/db" {
		t.Errorf("expected custom DBURL, got: %s", cfg.DBURL)
	}
	if cfg.MongoURI != "mongodb://custom:27017" {
		t.Errorf("expected custom MongoURI, got: %s", cfg.MongoURI)
	}
	if cfg.MongoDBName != "custom-db" {
		t.Errorf("expected custom MongoDBName, got: %s", cfg.MongoDBName)
	}
	if cfg.PollInterval != 10*time.Second {
		t.Errorf("expected 10s PollInterval, got: %v", cfg.PollInterval)
	}
	if cfg.MappingsPath != "/custom/mappings" {
		t.Errorf("expected custom MappingsPath, got: %s", cfg.MappingsPath)
	}
	if cfg.ViewsPath != "/custom/views" {
		t.Errorf("expected custom ViewsPath, got: %s", cfg.ViewsPath)
	}
	if cfg.HasuraURL != "http://hasura:8080" {
		t.Errorf("expected custom HasuraURL, got: %s", cfg.HasuraURL)
	}
	if cfg.HasuraSecret != "secret123" {
		t.Errorf("expected custom HasuraSecret")
	}
	if cfg.HasuraSource != "custom_source" {
		t.Errorf("expected custom HasuraSource, got: %s", cfg.HasuraSource)
	}
}

func TestLoadConfig_InvalidPollInterval(t *testing.T) {
	clearEnvVars()

	os.Setenv("POLL_INTERVAL_SEC", "invalid")
	defer clearEnvVars()

	cfg := LoadConfig()

	// Should fall back to default
	if cfg.PollInterval != 5*time.Second {
		t.Errorf("expected default PollInterval on invalid input, got: %v", cfg.PollInterval)
	}
}

func TestLoadConfig_ZeroPollInterval(t *testing.T) {
	clearEnvVars()

	os.Setenv("POLL_INTERVAL_SEC", "0")
	defer clearEnvVars()

	cfg := LoadConfig()

	// Zero should fall back to default (code checks parsed > 0)
	if cfg.PollInterval != 5*time.Second {
		t.Errorf("expected default PollInterval on zero input, got: %v", cfg.PollInterval)
	}
}

func TestLoadConfig_NegativePollInterval(t *testing.T) {
	clearEnvVars()

	os.Setenv("POLL_INTERVAL_SEC", "-5")
	defer clearEnvVars()

	cfg := LoadConfig()

	// Negative should fall back to default
	if cfg.PollInterval != 5*time.Second {
		t.Errorf("expected default PollInterval on negative input, got: %v", cfg.PollInterval)
	}
}

func TestLoadConfig_LargePollInterval(t *testing.T) {
	clearEnvVars()

	os.Setenv("POLL_INTERVAL_SEC", "3600")
	defer clearEnvVars()

	cfg := LoadConfig()

	// Should accept large values
	if cfg.PollInterval != 3600*time.Second {
		t.Errorf("expected 3600s PollInterval, got: %v", cfg.PollInterval)
	}
}

func TestLoadConfig_PartialOverrides(t *testing.T) {
	clearEnvVars()

	// Only set some values
	os.Setenv("DATABASE_URL", "postgres://partial:5432/db")
	os.Setenv("HASURA_URL", "http://partial-hasura:8080")
	defer clearEnvVars()

	cfg := LoadConfig()

	// Overridden values
	if cfg.DBURL != "postgres://partial:5432/db" {
		t.Errorf("expected partial DBURL override, got: %s", cfg.DBURL)
	}
	if cfg.HasuraURL != "http://partial-hasura:8080" {
		t.Errorf("expected partial HasuraURL override, got: %s", cfg.HasuraURL)
	}

	// Default values should still apply
	if cfg.MongoURI != "mongodb://mongo_vsc:27017" {
		t.Errorf("expected default MongoURI, got: %s", cfg.MongoURI)
	}
	if cfg.PollInterval != 5*time.Second {
		t.Errorf("expected default PollInterval, got: %v", cfg.PollInterval)
	}
}

func TestLoadConfig_EmptyStringValues(t *testing.T) {
	clearEnvVars()

	// Set empty strings explicitly
	os.Setenv("DATABASE_URL", "")
	os.Setenv("MONGO_URI", "")
	defer clearEnvVars()

	cfg := LoadConfig()

	// Empty strings should trigger defaults
	if cfg.DBURL != "postgres://indexer:indexerpass@localhost:5432/indexerdb?sslmode=disable" {
		t.Errorf("expected default DBURL for empty string, got: %s", cfg.DBURL)
	}
	if cfg.MongoURI != "mongodb://mongo_vsc:27017" {
		t.Errorf("expected default MongoURI for empty string, got: %s", cfg.MongoURI)
	}
}

func TestInit(t *testing.T) {
	clearEnvVars()

	// Set a known value
	os.Setenv("MONGO_DB_NAME", "test-init-db")
	defer clearEnvVars()

	Init()

	// Global should be populated
	if Global.MongoDBName != "test-init-db" {
		t.Errorf("expected Global.MongoDBName to be set by Init(), got: %s", Global.MongoDBName)
	}
}
