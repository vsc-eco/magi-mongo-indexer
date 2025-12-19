package hasura

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/tibfox/magi-mongo-indexer/internal/config"
	"github.com/tibfox/magi-mongo-indexer/internal/indexer/types"
)

// gqlRequest sends a GraphQL request (queries/mutations) to a Hasura/GraphQL endpoint.
func GqlRequest(url, adminSecret string, payload []byte) ([]byte, error) {
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if adminSecret != "" {
		req.Header.Set("X-Hasura-Admin-Secret", adminSecret)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

// HasuraMetadataRequest sends a request to Hasura's metadata API.
func HasuraMetadataRequest(url, adminSecret string, payload []byte) ([]byte, error) {
	if !strings.HasSuffix(url, "/v1/metadata") {
		url = strings.TrimSuffix(url, "/") + "/v1/metadata"
	}
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if adminSecret != "" {
		req.Header.Set("X-Hasura-Admin-Secret", adminSecret)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var errResp struct {
		Error string `json:"error"`
		Code  string `json:"code"`
	}
	if json.Unmarshal(b, &errResp) == nil && errResp.Error != "" {
		return nil, fmt.Errorf("hasura error [%s]: %s", errResp.Code, errResp.Error)
	}
	return b, nil
}

// dropInconsistentMetadata logs and clears Hasura's inconsistent metadata.
// It verifies afterwards that inconsistencies are really gone.
func dropInconsistentMetadata(url, adminSecret string) error {
	// Fetch inconsistencies
	getPayload, _ := json.Marshal(map[string]any{
		"type": "get_inconsistent_metadata",
		"args": map[string]any{},
	})
	resp, err := HasuraMetadataRequest(url, adminSecret, getPayload)
	if err != nil {
		return fmt.Errorf("failed to fetch inconsistent metadata: %w", err)
	}

	var inconsistencies []struct {
		Type   string `json:"type"`
		Name   string `json:"name"`
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal(resp, &inconsistencies); err == nil && len(inconsistencies) > 0 {
		log.Printf("[hasura] found %d inconsistent metadata objects:", len(inconsistencies))
		for _, inc := range inconsistencies {
			log.Printf("   - type=%s name=%s reason=%s", inc.Type, inc.Name, inc.Reason)
		}
	} else {
		if len(inconsistencies) == 0 {
			log.Printf("[hasura] no inconsistent metadata to drop")
			return nil
		}
	}

	// Drop inconsistencies
	dropPayload, _ := json.Marshal(map[string]any{
		"type": "drop_inconsistent_metadata",
		"args": map[string]any{},
	})
	if _, err := HasuraMetadataRequest(url, adminSecret, dropPayload); err != nil {
		return fmt.Errorf("failed to drop inconsistent metadata: %w", err)
	}
	log.Printf("[hasura] dropped inconsistent metadata")

	// Re-check
	resp, err = HasuraMetadataRequest(url, adminSecret, getPayload)
	if err != nil {
		return fmt.Errorf("failed to re-check inconsistent metadata: %w", err)
	}
	inconsistencies = nil
	if err := json.Unmarshal(resp, &inconsistencies); err == nil && len(inconsistencies) > 0 {
		return fmt.Errorf("inconsistent metadata remains after drop: %+v", inconsistencies)
	}
	return nil
}

// SyncHasuraTablesAndViews ensures Hasura tracks only the tables and views
// defined in mappings.yaml and views.yaml (plus contract_logs).
func SyncHasuraTablesAndViews(mappings *types.MappingFile, views *types.ViewsFile, cfg config.Config) error {
	return syncHasuraInternal(mappings, views, true, cfg)
}

// internal wrapper with retry logic
func syncHasuraInternal(mappings *types.MappingFile, views *types.ViewsFile, allowRetry bool, cfg config.Config) error {
	hasuraURL := cfg.HasuraURL
	adminSecret := cfg.HasuraSecret
	if hasuraURL == "" || adminSecret == "" {
		log.Println("[hasura] skipping auto-sync (missing HASURA_URL or HASURA_ADMIN_SECRET)")
		return nil
	}

	desiredTables, desiredViews, trackedTables, trackedViews, err := GetDesiredAndTracked(mappings, views, cfg)
	if err != nil {
		if allowRetry && strings.Contains(err.Error(), "inconsistent metadata") {
			log.Printf("[hasura] detected inconsistent metadata, dropping…")
			if derr := dropInconsistentMetadata(hasuraURL, adminSecret); derr != nil {
				return derr
			}
			return syncHasuraInternal(mappings, views, false, cfg)
		}
		return err
	}

	// --- Sync tables ---
	for t := range desiredTables {
		if _, ok := trackedTables[t]; !ok {
			if err := trackTable(hasuraURL, adminSecret, t, cfg.HasuraSource); err != nil {
				if allowRetry && strings.Contains(err.Error(), "inconsistent metadata") {
					log.Printf("[hasura] inconsistent metadata while tracking %s, dropping…", t)
					if derr := dropInconsistentMetadata(hasuraURL, adminSecret); derr != nil {
						return derr
					}
					return syncHasuraInternal(mappings, views, false, cfg)
				}
				return fmt.Errorf("failed to track table %s: %w", t, err)
			}
			log.Printf("[hasura] tracked new table: %s", t)
			// Grant public select + subscription permissions
			if err := grantPublicSelectPermission(hasuraURL, adminSecret, t, cfg.HasuraSource, false); err != nil {
				log.Printf("[hasura] warning: failed to set public permissions for table %s: %v", t, err)
			}

		}
	}
	for t := range trackedTables {
		if _, ok := desiredTables[t]; !ok && !strings.HasPrefix(t, "cst_") {
			if err := untrackTable(hasuraURL, adminSecret, t, cfg.HasuraSource); err != nil {
				return fmt.Errorf("failed to untrack table %s: %w", t, err)
			}
			log.Printf("[hasura] untracked removed table: %s", t)
		}
	}

	// --- Sync views ---
	for v := range desiredViews {
		if _, ok := trackedViews[v]; !ok {
			if err := trackTable(hasuraURL, adminSecret, v, cfg.HasuraSource); err != nil {
				if allowRetry && strings.Contains(err.Error(), "inconsistent metadata") {
					log.Printf("[hasura] inconsistent metadata while tracking view %s, dropping…", v)
					if derr := dropInconsistentMetadata(hasuraURL, adminSecret); derr != nil {
						return derr
					}
					return syncHasuraInternal(mappings, views, false, cfg)
				}
				return fmt.Errorf("failed to track view %s: %w", v, err)
			}
			log.Printf("[hasura] tracked new view: %s", v)

			if err := grantPublicSelectPermission(hasuraURL, adminSecret, v, cfg.HasuraSource, true); err != nil {
				log.Printf("[hasura] warning: failed to set public permissions for view %s: %v", v, err)
			}

		}
	}
	for v := range trackedViews {
		if _, ok := desiredViews[v]; !ok && !strings.HasPrefix(v, "cst_") {
			if err := untrackTable(hasuraURL, adminSecret, v, cfg.HasuraSource); err != nil {
				return fmt.Errorf("failed to untrack view %s: %w", v, err)
			}
			log.Printf("[hasura] untracked removed view: %s", v)
		}
	}
	return nil
}

// GetDesiredAndTracked exports metadata and compares with yaml config.
func GetDesiredAndTracked(mappings *types.MappingFile, views *types.ViewsFile, cfg config.Config) (
	desiredTables map[string]struct{},
	desiredViews map[string]struct{},
	trackedTables map[string]struct{},
	trackedViews map[string]struct{},
	err error,
) {
	desiredTables = make(map[string]struct{})
	desiredViews = make(map[string]struct{})

	// Always include generic log table
	desiredTables["contract_logs"] = struct{}{}

	// nested loop for the new mappings structure
	for _, contract := range mappings.Contracts {
		for _, m := range contract.Events {
			desiredTables[m.Table] = struct{}{}
		}
	}

	// Collect all desired views
	for _, v := range views.Views {
		desiredViews[v.Name] = struct{}{}
	}

	hasuraURL := cfg.HasuraURL
	adminSecret := cfg.HasuraSecret
	if hasuraURL == "" || adminSecret == "" {
		return desiredTables, desiredViews, map[string]struct{}{}, map[string]struct{}{}, nil
	}

	body, _ := json.Marshal(map[string]any{
		"type": "export_metadata",
		"args": map[string]any{},
	})
	b, err := HasuraMetadataRequest(hasuraURL, adminSecret, body)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("failed to fetch metadata: %w", err)
	}

	var meta struct {
		Sources []struct {
			Name   string `json:"name"`
			Tables []struct {
				Table struct {
					Name   string `json:"name"`
					Schema string `json:"schema"`
				} `json:"table"`
			} `json:"tables"`
		} `json:"sources"`
	}
	if err := json.Unmarshal(b, &meta); err != nil {
		return nil, nil, nil, nil, fmt.Errorf("failed to parse metadata: %w", err)
	}

	trackedTables = make(map[string]struct{})
	trackedViews = make(map[string]struct{})

	for _, s := range meta.Sources {
		if s.Name == cfg.HasuraSource {
			for _, t := range s.Tables {
				if t.Table.Schema != "public" {
					continue
				}
				if _, isView := desiredViews[t.Table.Name]; isView {
					trackedViews[t.Table.Name] = struct{}{}
				} else {
					trackedTables[t.Table.Name] = struct{}{}
				}
			}
		}
	}
	return desiredTables, desiredViews, trackedTables, trackedViews, nil
}

// --- helpers ---
func trackTable(url, secret, name string, hasuraSource string) error {
	payload, _ := json.Marshal(map[string]any{
		"type": "pg_track_table",
		"args": map[string]any{
			"source": hasuraSource,
			"table": map[string]string{
				"schema": "public",
				"name":   name,
			},
		},
	})
	_, err := HasuraMetadataRequest(url, secret, payload)
	return err
}

func untrackTable(url, secret, name string, hasuraSource string) error {
	payload, _ := json.Marshal(map[string]any{
		"type": "pg_untrack_table",
		"args": map[string]any{
			"source": hasuraSource,
			"table": map[string]string{
				"schema": "public",
				"name":   name,
			},
		},
	})
	_, err := HasuraMetadataRequest(url, secret, payload)
	return err
}

func grantPublicSelectPermission(url, secret, name, hasuraSource string, isView bool) error {
	// Determine which query root fields to include.
	// Views don't have primary keys, so "select_by_pk" must be excluded.
	queryFields := []string{"select", "select_aggregate"}
	if !isView {
		queryFields = append(queryFields, "select_by_pk")
	}

	payload := map[string]any{
		"type": "pg_create_select_permission",
		"args": map[string]any{
			"source": hasuraSource,
			"table": map[string]string{
				"schema": "public",
				"name":   name,
			},
			"role": "public",
			"permission": map[string]any{
				"columns":            "*",
				"filter":             map[string]any{},
				"allow_aggregations": true,
				"query_root_fields":  queryFields,
				"subscription_root_fields": []string{
					"select", "select_stream",
				},
			},
		},
	}

	body, _ := json.Marshal(payload)
	resp, err := HasuraMetadataRequest(url, secret, body)
	if err != nil {
		// Hasura sometimes rejects because of invalid fields (e.g. select_by_pk)
		// Log more clearly for diagnosis.
		return fmt.Errorf("[hasura] failed to create public select permission for %s: %w", name, err)
	}

	// Optional extra validation: Hasura returns errors even with 200 sometimes.
	var errResp struct {
		Error string `json:"error"`
		Code  string `json:"code"`
	}
	if json.Unmarshal(resp, &errResp) == nil && errResp.Error != "" {
		return fmt.Errorf("[hasura] permission creation error on %s [%s]: %s", name, errResp.Code, errResp.Error)
	}

	log.Printf("[hasura] granted public select permission on %s (isView=%v)", name, isView)
	return nil
}
