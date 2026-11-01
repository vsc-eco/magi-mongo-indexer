package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	_ "github.com/lib/pq"

	"github.com/tibfox/magi-mongo-indexer/internal/config"
	"github.com/tibfox/magi-mongo-indexer/internal/indexer/backfill"
	"github.com/tibfox/magi-mongo-indexer/internal/indexer/datalayer"
	"github.com/tibfox/magi-mongo-indexer/internal/indexer/fetcher"
	"github.com/tibfox/magi-mongo-indexer/internal/indexer/hasura"
	"github.com/tibfox/magi-mongo-indexer/internal/indexer/mapper"
)

// withPgTimeouts adds a server-side statement_timeout and a connect_timeout to
// the Postgres DSN so no single query or connect attempt can block the
// single-threaded indexer loop indefinitely. lib/pq forwards statement_timeout
// as a connection runtime parameter, so it applies to every pooled connection
// and every query (db.Exec, datalayer.InsertLog, discovery writes, …) without
// touching each call site. Mirrors the mongo-side timeouts in the fetcher.
func withPgTimeouts(dsn string) string {
	if dsn == "" {
		return dsn
	}
	if strings.Contains(dsn, "://") { // URL form: postgres://user:pass@host/db
		sep := "?"
		if strings.Contains(dsn, "?") {
			sep = "&"
		}
		return dsn + sep + "statement_timeout=30000&connect_timeout=10"
	}
	// key=value form: "host=… dbname=…"
	return dsn + " statement_timeout=30000 connect_timeout=10"
}

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
	db, err := sql.Open("postgres", withPgTimeouts(cfg.DBURL))
	if err != nil {
		log.Fatal("❌ db connect failed:", err)
	}
	defer db.Close()

	// --- Ensure schema and sync Hasura ---
	if err := datalayer.EnsureTables(db, mappings); err != nil {
		log.Fatal("❌ failed to ensure tables:", err)
	}

	// --- Initialize contract discovery (before views, since views may reference discovered_contracts) ---
	if err := mapper.EnsureDiscoveredContractsTable(db); err != nil {
		log.Fatal("❌ failed to ensure discovered_contracts table:", err)
	}
	if err := mapper.LoadDiscoveredContracts(db); err != nil {
		log.Fatal("❌ failed to load discovered contracts:", err)
	}

	if err := datalayer.EnsureViews(db, views); err != nil {
		log.Fatal("❌ failed to ensure views:", err)
	}
	if err := datalayer.EnsureHealthView(db); err != nil {
		log.Fatal("❌ failed to ensure health view:", err)
	}

	if err := hasura.SyncHasuraTablesAndViews(mappings, views, cfg); err != nil {
		log.Fatal("❌ failed to sync tables/views in Hasura:", err)
	}
	log.Printf("[startup] ✅ Initial Hasura metadata sync complete")

	// --- Optional backfill: re-parse the last N blocks of raw logs so rows
	//     with NULL schema columns (e.g. after a mapping added a new variant
	//     or column) get fixed without a full re-index.
	if cfg.BackfillBlocks > 0 {
		log.Printf("[backfill] scanning last %d blocks for NULL columns", cfg.BackfillBlocks)
		if err := backfill.Run(db, mappings, cfg.BackfillBlocks); err != nil {
			log.Printf("[backfill] error: %v", err)
		}
	}

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
