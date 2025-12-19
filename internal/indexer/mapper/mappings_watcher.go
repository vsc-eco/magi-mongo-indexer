package mapper

import (
	"database/sql"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/tibfox/magi-mongo-indexer/internal/config"
	"github.com/tibfox/magi-mongo-indexer/internal/indexer/datalayer"
	"github.com/tibfox/magi-mongo-indexer/internal/indexer/hasura"
)

// ReloadCallback is called after successful reload
type ReloadCallback func()

func WatchAndSync(db *sql.DB, mappingsPath, viewsPath string, cfg config.Config, onReload ReloadCallback) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer watcher.Close()

	// Watch the directory if it's a folder
	mappingDir := mappingsPath
	viewDir := viewsPath
	if info, err := os.Stat(mappingsPath); err == nil && !info.IsDir() {
		mappingDir = filepath.Dir(mappingsPath)
	}
	if info, err := os.Stat(viewsPath); err == nil && !info.IsDir() {
		viewDir = filepath.Dir(viewsPath)
	}

	if err := watcher.Add(mappingDir); err != nil {
		return err
	}
	if viewDir != mappingDir {
		_ = watcher.Add(viewDir)
	}

	var (
		debounceMu    sync.Mutex
		debounceTimer *time.Timer
	)

	triggerReload := func(reason string) {
		log.Printf("[watcher] reloading due to %s", reason)

		mappings, err := LoadMappings(mappingsPath)
		if err != nil {
			log.Printf("[watcher] failed to reload mappings: %v", err)
			return
		}
		views, err := LoadViews(viewsPath)
		if err != nil {
			log.Printf("[watcher] failed to reload views: %v", err)
			return
		}

		UpdateState(mappings, views)

		if err := datalayer.EnsureTables(db, mappings); err != nil {
			log.Printf("[watcher] failed to ensure tables: %v", err)
		}
		if err := datalayer.EnsureViews(db, views); err != nil {
			log.Printf("[watcher] failed to ensure views: %v", err)
		}

		if err := hasura.SyncHasuraTablesAndViews(mappings, views, cfg); err != nil {
			log.Printf("[watcher] failed to sync Hasura metadata: %v", err)
		} else {
			log.Printf("[watcher] ✅ DB schema + Hasura synced")
		}

		if onReload != nil {
			onReload()
		}
	}

	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				return nil
			}
			if event.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Remove|fsnotify.Rename) != 0 {
				if strings.HasSuffix(event.Name, "_mappings.yaml") || strings.HasSuffix(event.Name, "_views.yaml") {
					log.Printf("[watcher] detected change in %s", event.Name)
					debounceMu.Lock()
					if debounceTimer != nil {
						debounceTimer.Stop()
					}
					debounceTimer = time.AfterFunc(1*time.Second, func() {
						triggerReload("fsnotify")
					})
					debounceMu.Unlock()
				}
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return nil
			}
			log.Printf("[watcher] error: %v", err)
		}
	}
}
