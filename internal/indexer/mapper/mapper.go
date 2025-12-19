package mapper

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"

	"github.com/tibfox/magi-mongo-indexer/internal/indexer/types"
	"gopkg.in/yaml.v3"
)

// LoadMappings merges all *_mappings.yaml files in the given directory
func LoadMappings(path string) (*types.MappingFile, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	combined := &types.MappingFile{}
	if info.IsDir() {
		entries, err := os.ReadDir(path)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), "_mappings.yaml") {
				continue
			}
			data, err := os.ReadFile(filepath.Join(path, e.Name()))
			if err != nil {
				return nil, err
			}
			var mf types.MappingFile
			if err := yaml.Unmarshal(data, &mf); err != nil {
				return nil, err
			}
			combined.Contracts = append(combined.Contracts, mf.Contracts...)
		}
	} else {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		if err := yaml.Unmarshal(data, combined); err != nil {
			return nil, err
		}
	}
	return combined, nil
}

// FindMapping finds the EventMapping for a given contract address and log string.
// It supports both JSON ("type" field) and CSV (first token) log formats.
func FindMapping(addr string, logStr string) *types.EventMapping {
	mappings := GetMappings()
	if mappings == nil {
		return nil
	}

	for _, c := range mappings.Contracts {
		if c.Address != addr {
			continue
		}

		for _, m := range c.Events {
			// JSON logs
			if m.Parse == "json" && strings.HasPrefix(strings.TrimSpace(logStr), "{") {
				var raw map[string]interface{}
				if err := json.Unmarshal([]byte(logStr), &raw); err == nil {
					if t, ok := raw["type"].(string); ok && t == m.LogType {
						return &m
					}
				}
			}

			// CSV logs
			if m.Parse == "csv" {
				delim := m.Delimiter
				if delim == "" {
					delim = " "
				}
				parts := strings.Split(logStr, delim)
				if len(parts) > 0 && strings.TrimSpace(parts[0]) == m.LogType {
					return &m
				}
			}
		}
	}
	return nil
}

// LoadViews merges all *_views.yaml files in the given directory
func LoadViews(path string) (*types.ViewsFile, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	combined := &types.ViewsFile{}
	if info.IsDir() {
		entries, err := os.ReadDir(path)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), "_views.yaml") {
				continue
			}
			data, err := os.ReadFile(filepath.Join(path, e.Name()))
			if err != nil {
				return nil, err
			}
			var vf types.ViewsFile
			if err := yaml.Unmarshal(data, &vf); err != nil {
				return nil, err
			}
			combined.Views = append(combined.Views, vf.Views...)
		}
	} else {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		if err := yaml.Unmarshal(data, combined); err != nil {
			return nil, err
		}
	}
	return combined, nil
}
