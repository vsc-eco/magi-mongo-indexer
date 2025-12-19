package fetcher

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/tibfox/magi-mongo-indexer/internal/indexer/datalayer"
	"github.com/tibfox/magi-mongo-indexer/internal/indexer/mapper"
	"github.com/tibfox/magi-mongo-indexer/internal/indexer/types"
)

var (
	connMu   sync.Mutex
	currConn *websocket.Conn
	stopCh   = make(chan struct{})
)

// RestartWS closes the current connection, forcing HandleWS to reconnect.
func RestartWS() {
	connMu.Lock()
	defer connMu.Unlock()

	if currConn != nil {
		log.Println("[ws] 🔄 restarting subscription due to mappings/view change")
		_ = currConn.Close()
		currConn = nil
	}
}

// HandleWS manages a persistent WebSocket connection to the node GraphQL API.
// It auto-reconnects with backoff and performs live log insertion into Postgres.
func HandleWS(db *sql.DB, url string) error {
	backoff := time.Second * 5

	for {
		select {
		case <-stopCh:
			log.Println("[ws] stopping WS handler gracefully")
			return nil
		default:
		}

		log.Printf("[ws] connecting to %s ...", url)
		conn, _, err := websocket.DefaultDialer.Dial(url, nil)
		if err != nil {
			log.Printf("[ws] ❌ dial failed: %v", err)
			sleep := backoff + time.Duration(rand.Intn(3000))*time.Millisecond
			log.Printf("[ws] retrying in %v...", sleep)
			time.Sleep(sleep)
			if backoff < time.Minute {
				backoff *= 2
			}
			continue
		}

		// reset backoff after successful connect
		backoff = time.Second * 5

		connMu.Lock()
		currConn = conn
		connMu.Unlock()

		log.Println("[ws] ✅ connection established")

		runSubscription(db, conn)

		// If we exit runSubscription, conn was closed or errored.
		log.Println("[ws] connection closed, retrying...")
		time.Sleep(backoff)
	}
}

// runSubscription handles a single WebSocket session lifecycle.
func runSubscription(db *sql.DB, conn *websocket.Conn) {
	defer conn.Close()

	conn.SetPongHandler(func(appData string) error {
		// log.Println("[ws] pong received")
		return nil
	})

	// --- 1️⃣ Init connection
	initMsg := map[string]interface{}{
		"type":    "connection_init",
		"payload": map[string]interface{}{},
	}
	if err := conn.WriteJSON(initMsg); err != nil {
		log.Printf("[ws] init send error: %v", err)
		return
	}

	// Wait for ack
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, msg, err := conn.ReadMessage()
	if err != nil {
		log.Printf("[ws] read init ack failed: %v", err)
		return
	}
	var initFrame struct{ Type string }
	if err := json.Unmarshal(msg, &initFrame); err != nil || initFrame.Type != "connection_ack" {
		log.Printf("[ws] invalid connection_ack: %s", msg)
		return
	}
	// Clear the handshake deadline so steady-state reads don't time out implicitly.
	conn.SetReadDeadline(time.Time{})

	// --- 2️⃣ Prepare subscriptions
	subscription := `
	subscription ($filter: LogFilter) {
		logs(filter: $filter) {
			blockHeight
			txHash
			contractAddress
			log
			timestamp
		}
	}`

	mappings := mapper.GetMappings()
	if mappings == nil || len(mappings.Contracts) == 0 {
		log.Println("[ws] no contract mappings loaded, waiting before retry")
		time.Sleep(5 * time.Second)
		return
	}

	for _, c := range mappings.Contracts {
		filter := map[string]interface{}{
			"contractAddresses": []string{c.Address},
		}
		if c.FromBlockHeight != nil {
			filter["fromBlock"] = *c.FromBlockHeight
		}

		startMsg := map[string]interface{}{
			"id":   fmt.Sprintf("sub_%s", c.Address),
			"type": "start",
			"payload": map[string]interface{}{
				"query": subscription,
				"variables": map[string]interface{}{
					"filter": filter,
				},
			},
		}

		if err := conn.WriteJSON(startMsg); err != nil {
			log.Printf("[ws] failed to start subscription for %s: %v", c.Address, err)
		} else {
			log.Printf("[ws] 🛰️ subscription started for %s", c.Address)
		}
	}

	// --- 3️⃣ Keepalive ping loop
	go func(c *websocket.Conn) {
		ticker := time.NewTicker(20 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				connMu.Lock()
				if currConn != c {
					connMu.Unlock()
					return
				}
				if err := c.WriteControl(websocket.PingMessage, []byte("ping"), time.Now().Add(5*time.Second)); err != nil {
					log.Printf("[ws] ping failed: %v", err)
					connMu.Unlock()
					_ = c.Close()
					return
				}
				connMu.Unlock()
			case <-stopCh:
				return
			}
		}
	}(conn)

	// --- 4️⃣ Read incoming messages
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			log.Printf("[ws] read error: %v", err)
			return
		}

		var frame struct {
			Type    string          `json:"type"`
			Payload json.RawMessage `json:"payload"`
		}
		if err := json.Unmarshal(msg, &frame); err != nil {
			log.Printf("[ws] unmarshal error: %v", err)
			continue
		}

		switch frame.Type {
		case "ka":
			continue
		case "error":
			log.Printf("[ws] error frame: %s", string(frame.Payload))
			continue
		case "data":
			var payload struct {
				Data struct {
					Logs types.LogEvent `json:"logs"`
				} `json:"data"`
			}
			if err := json.Unmarshal(frame.Payload, &payload); err != nil {
				log.Printf("[ws] payload parse error: %v", err)
				continue
			}
			ev := payload.Data.Logs
			if (ev == types.LogEvent{}) {
				continue
			}

			// Insert raw log for traceability
			_, err := db.Exec(
				`INSERT INTO contract_logs (block_height, tx_hash, contract_address, log, ts)
				 VALUES ($1, $2, $3, $4, $5)
				 ON CONFLICT DO NOTHING`,
				ev.BlockHeight, ev.TxHash, ev.ContractAddress, ev.Log, ev.Timestamp,
			)
			if err != nil {
				log.Printf("[ws] insert contract_logs error: %v", err)
			}

			if mapping := mapper.FindMapping(ev.ContractAddress, ev.Log); mapping != nil {
				datalayer.InsertLog(db, *mapping, ev)
			}
		}
	}
}

// StopWS gracefully stops all running WebSocket connections.
func StopWS() {
	close(stopCh)
	connMu.Lock()
	defer connMu.Unlock()
	if currConn != nil {
		log.Println("[ws] closing active connection")
		_ = currConn.Close()
		currConn = nil
	}
}
