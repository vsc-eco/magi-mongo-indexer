package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
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

// healthStatus represents the response from the health check endpoint
type healthStatus struct {
	Status   string `json:"status"`
	Postgres bool   `json:"postgres"`
	Mappings bool   `json:"mappings"`
}

// healthHandler handles health check requests
type healthHandler struct {
	db *sql.DB
}

func (h *healthHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	status := healthStatus{
		Status:   "healthy",
		Postgres: false,
		Mappings: false,
	}

	// Check Postgres connection
	if err := h.db.Ping(); err == nil {
		status.Postgres = true
	}

	// Check mappings loaded
	if mapper.GetMappings() != nil {
		status.Mappings = true
	}

	// Set overall status
	if !status.Postgres || !status.Mappings {
		status.Status = "unhealthy"
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
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

	// --- Health check HTTP server ---
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/health", &healthHandler{db: db})

		port := os.Getenv("HEALTH_PORT")
		if port == "" {
			port = "8080"
		}

		log.Printf("[health] starting health check server on :%s", port)
		if err := http.ListenAndServe(":"+port, mux); err != nil {
			log.Printf("[health] server error: %v", err)
		}
	}()

	// --- Wait for Ctrl+C ---
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	<-interrupt

	fmt.Println("🛑 Stopping indexer...")
}
