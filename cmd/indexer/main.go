package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	_ "github.com/lib/pq"

	"github.com/tibfox/magi-mongo-indexer/internal/config"
	"github.com/tibfox/magi-mongo-indexer/internal/indexer/datalayer"
	"github.com/tibfox/magi-mongo-indexer/internal/indexer/fetcher"
	"github.com/tibfox/magi-mongo-indexer/internal/indexer/hasura"
	"github.com/tibfox/magi-mongo-indexer/internal/indexer/mapper"
)

func main() {
	cfg := config.LoadConfig()

	// --- Initial mappings/views load ---
	mappings, err := mapper.LoadMappings(cfg.MappingsPath)
	if err != nil {
		log.Fatal("❌ failed to load mappings:", err)
	}
	views, err := mapper.LoadViews(cfg.ViewsPath)
	if err != nil {
		log.Fatal("❌ failed to load views:", err)
	}
	mapper.UpdateState(mappings, views)

	// --- Connect to Postgres ---
	db, err := sql.Open("postgres", cfg.DBURL)
	if err != nil {
		log.Fatal("❌ db connect failed:", err)
	}
	defer db.Close()

	// --- Ensure schema and sync Hasura ---
	if err := datalayer.EnsureTables(db, mappings); err != nil {
		log.Fatal("❌ failed to ensure tables:", err)
	}
	if err := datalayer.EnsureViews(db, views); err != nil {
		log.Fatal("❌ failed to ensure views:", err)
	}
	if err := hasura.SyncHasuraTablesAndViews(mappings, views, cfg); err != nil {
		log.Fatal("❌ failed to sync tables/views in Hasura:", err)
	}
	log.Printf("[startup] ✅ Initial Hasura metadata sync complete")

	// --- Watcher loop (recovers automatically) ---
	go func() {
		backoff := time.Second * 5
		for {
			log.Printf("[watcher] starting config watcher for %s", cfg.MappingsPath)
			err := mapper.WatchAndSync(
				db,
				cfg.MappingsPath,
				cfg.ViewsPath,
				cfg,
				func() {
					log.Println("[watcher] 🔄 Config reloaded — mappings updated")
				},
			)
			if err != nil {
				log.Printf("[watcher] error: %v", err)
				log.Printf("[watcher] retrying in %v...", backoff)
				time.Sleep(backoff)
				if backoff < time.Minute*5 {
					backoff *= 2
				}
				continue
			}
			backoff = time.Second * 5
		}
	}()

	// --- MongoDB polling loop (recovers automatically) ---
	go func() {
		backoff := time.Second * 5
		for {
			log.Println("[mongo] starting MongoDB polling...")
			err := fetcher.HandleMongo(db, cfg.MongoURI, cfg.MongoDBName, cfg.PollInterval)
			if err != nil {
				log.Printf("[mongo] error: %v", err)
				log.Printf("[mongo] retrying in %v...", backoff)
				time.Sleep(backoff)
				if backoff < time.Minute*5 {
					backoff *= 2
				}
				continue
			}
			backoff = time.Second * 5
		}
	}()

	// --- Wait for Ctrl+C ---
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	<-interrupt

	fmt.Println("🛑 Stopping indexer...")
}
