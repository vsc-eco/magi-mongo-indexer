package fetcher

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/tibfox/magi-mongo-indexer/internal/indexer/datalayer"
	"github.com/tibfox/magi-mongo-indexer/internal/indexer/mapper"
	"github.com/tibfox/magi-mongo-indexer/internal/indexer/types"
)

// MongoContractState represents the document structure in the contract_state collection
type MongoContractState struct {
	ID          interface{}   `bson:"_id"`
	DocID       string        `bson:"id"`
	BlockHeight uint64        `bson:"block_height"`
	ContractID  string        `bson:"contract_id"`
	Inputs      []string      `bson:"inputs"`
	Results     []Result      `bson:"results"`
}

// Result represents the execution result with logs
type Result struct {
	Ret  string   `bson:"ret"`
	OK   bool     `bson:"ok"`
	Logs []string `bson:"logs"`
}

// BlockHeader represents a block header from the block_headers collection
type BlockHeader struct {
	StartBlock uint64 `bson:"start_block"`
	EndBlock   uint64 `bson:"end_block"`
	Timestamp  string `bson:"ts"`
}

// TransactionPoolRecord represents a document from the transaction_pool collection
type TransactionPoolRecord struct {
	Id             string `bson:"id"`
	AnchoredHeight uint64 `bson:"anchr_height"`
}

// HiveBlockDocument represents a document from the hive_blocks collection
type HiveBlockDocument struct {
	Block *HiveBlock `bson:"block,omitempty"`
}

// HiveBlock represents the nested block data in hive_blocks
type HiveBlock struct {
	BlockNumber uint64 `bson:"block_number"`
	Timestamp   string `bson:"timestamp"`
}

// HandleMongo manages a connection to MongoDB and polls the contract_state collection
// for new entries, inserting them into Postgres.
func HandleMongo(db *sql.DB, mongoURI string, dbName string, pollInterval time.Duration) error {
	ctx := context.Background()

	// Connect to MongoDB
	clientOptions := options.Client().ApplyURI(mongoURI)
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return fmt.Errorf("failed to connect to MongoDB: %w", err)
	}
	defer client.Disconnect(ctx)

	// Ping to verify connection
	if err := client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	log.Printf("[mongo] ✅ connected to MongoDB at %s", mongoURI)

	contractStateCol := client.Database(dbName).Collection("contract_state")
	blockHeadersCol := client.Database(dbName).Collection("block_headers")
	txPoolCol := client.Database(dbName).Collection("transaction_pool")
	hiveBlocksCol := client.Database(dbName).Collection("hive_blocks")

	// Track the last processed block height per contract
	lastProcessed := make(map[string]uint64)

	// Initialize last processed heights from the database
	mappings := mapper.GetMappings()
	if mappings != nil {
		for _, contract := range mappings.Contracts {
			if contract.Address == "" {
				continue // skip templates (no static address)
			}
			lastBlock := datalayer.GetLastIndexedBlock(db, contract.Address)
			if lastBlock > 0 {
				lastProcessed[contract.Address] = uint64(lastBlock)
			} else if contract.FromBlockHeight != nil {
				lastProcessed[contract.Address] = *contract.FromBlockHeight
			} else {
				lastProcessed[contract.Address] = 0
			}
			log.Printf("[mongo] initialized last processed block for %s: %d", contract.Address, lastProcessed[contract.Address])
		}

		// Initialize last processed for discovered contracts
		for _, contract := range mappings.Contracts {
			if contract.DiscoverEvent == "" {
				continue
			}
			discovered := mapper.GetDiscoveredContracts(contract.DiscoverEvent)
			for _, addr := range discovered {
				lastBlock := datalayer.GetLastIndexedBlock(db, addr)
				if lastBlock > 0 {
					lastProcessed[addr] = uint64(lastBlock)
				} else {
					lastProcessed[addr] = 0
				}
				log.Printf("[mongo] initialized last processed block for discovered contract %s: %d", addr, lastProcessed[addr])
			}
		}
	}

	// Track last scanned block for discovery
	lastDiscoveryScan := make(map[string]uint64)

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	log.Printf("[mongo] starting polling loop (interval: %v)", pollInterval)

	for {
		select {
		case <-ticker.C:
			// Refresh mappings in case they changed
			mappings := mapper.GetMappings()
			if mappings == nil || len(mappings.Contracts) == 0 {
				log.Println("[mongo] no contract mappings loaded, skipping poll")
				continue
			}

			// Phase 1: Discovery scan — find new contracts emitting discover events
			for _, contract := range mappings.Contracts {
				if contract.DiscoverEvent == "" || contract.Address != "" {
					continue
				}
				if err := scanForDiscovery(ctx, db, contractStateCol, blockHeadersCol, txPoolCol, hiveBlocksCol, contract, lastDiscoveryScan); err != nil {
					log.Printf("[mongo] discovery scan error for event %s: %v", contract.DiscoverEvent, err)
				}
			}

			// Phase 2: Process each static contract
			for _, contract := range mappings.Contracts {
				if contract.Address == "" {
					continue // skip templates
				}
				if err := processContract(ctx, db, contractStateCol, blockHeadersCol, txPoolCol, hiveBlocksCol, contract, lastProcessed); err != nil {
					log.Printf("[mongo] error processing contract %s: %v", contract.Address, err)
				}
			}

			// Phase 3: Process discovered contracts using their template mappings
			for _, contract := range mappings.Contracts {
				if contract.DiscoverEvent == "" || contract.Address != "" {
					continue
				}
				discovered := mapper.GetDiscoveredContracts(contract.DiscoverEvent)
				for _, addr := range discovered {
					// Create a virtual contract mapping with the discovered address
					virtualContract := types.ContractMapping{
						Address:         addr,
						FromBlockHeight: contract.FromBlockHeight,
						Events:          contract.Events,
					}
					if err := processContract(ctx, db, contractStateCol, blockHeadersCol, txPoolCol, hiveBlocksCol, virtualContract, lastProcessed); err != nil {
						log.Printf("[mongo] error processing discovered contract %s: %v", addr, err)
					}
				}
			}
		}
	}
}

// getBlockTimestamp fetches the timestamp for a given block height from block_headers
func getBlockTimestamp(ctx context.Context, blockHeadersCol *mongo.Collection, blockHeight uint64) (string, error) {
	filter := bson.M{
		"start_block": bson.M{"$lte": blockHeight},
		"end_block":   bson.M{"$gte": blockHeight},
	}

	var header BlockHeader
	err := blockHeadersCol.FindOne(ctx, filter).Decode(&header)
	if err != nil {
		return "", fmt.Errorf("failed to find block header for height %d: %w", blockHeight, err)
	}

	return header.Timestamp, nil
}

// getHiveBlockTimestamp fetches the timestamp for a given block height from hive_blocks
func getHiveBlockTimestamp(ctx context.Context, hiveBlocksCol *mongo.Collection, blockHeight uint64) (string, error) {
	var doc HiveBlockDocument
	err := hiveBlocksCol.FindOne(ctx, bson.M{"block.block_number": blockHeight}).Decode(&doc)
	if err != nil {
		return "", fmt.Errorf("failed to find hive block for height %d: %w", blockHeight, err)
	}
	if doc.Block == nil {
		return "", fmt.Errorf("hive block document has no block data for height %d", blockHeight)
	}
	return doc.Block.Timestamp, nil
}

// processContract fetches new entries for a specific contract and processes them
func processContract(
	ctx context.Context,
	db *sql.DB,
	contractStateCol *mongo.Collection,
	blockHeadersCol *mongo.Collection,
	txPoolCol *mongo.Collection,
	hiveBlocksCol *mongo.Collection,
	contract types.ContractMapping,
	lastProcessed map[string]uint64,
) error {
	// Build query filter - use contract_id and look for documents with logs
	filter := bson.M{
		"contract_id":  contract.Address,
		"block_height": bson.M{"$gt": lastProcessed[contract.Address]},
		"results.logs": bson.M{"$exists": true, "$not": bson.M{"$size": 0}},
	}

	// Sort by block height ascending
	findOptions := options.Find().SetSort(bson.D{{Key: "block_height", Value: 1}})

	cursor, err := contractStateCol.Find(ctx, filter, findOptions)
	if err != nil {
		return fmt.Errorf("failed to query MongoDB: %w", err)
	}
	defer cursor.Close(ctx)

	processedCount := 0
	logsProcessed := 0
	var maxBlockHeight uint64 = lastProcessed[contract.Address]

	for cursor.Next(ctx) {
		var doc MongoContractState
		if err := cursor.Decode(&doc); err != nil {
			log.Printf("[mongo] failed to decode document: %v", err)
			continue
		}

		// Process each result that has logs, matching with corresponding input
		for resultIdx, result := range doc.Results {
			if len(result.Logs) == 0 {
				continue
			}

			// Determine the input tx hash, block height, and timestamp for this result
			var txHash string
			var blockHeight uint64

			if resultIdx < len(doc.Inputs) {
				txHash = doc.Inputs[resultIdx]

				// Look up the inclusion height from transaction_pool
				var txRecord TransactionPoolRecord
				err := txPoolCol.FindOne(ctx, bson.M{"id": txHash}).Decode(&txRecord)
				if err != nil {
					log.Printf("[mongo] failed to look up tx %s in transaction_pool: %v, falling back to doc block height", txHash, err)
					blockHeight = doc.BlockHeight
				} else {
					blockHeight = txRecord.AnchoredHeight
				}
			} else {
				log.Printf("[mongo] result index %d exceeds inputs length %d for doc %s, falling back", resultIdx, len(doc.Inputs), doc.DocID)
				txHash = doc.DocID
				blockHeight = doc.BlockHeight
			}

			// Get timestamp from hive_blocks, fall back to block_headers
			timestamp, err := getHiveBlockTimestamp(ctx, hiveBlocksCol, blockHeight)
			if err != nil {
				timestamp, err = getBlockTimestamp(ctx, blockHeadersCol, blockHeight)
				if err != nil {
					log.Printf("[mongo] failed to get timestamp for block %d: %v, using current time", blockHeight, err)
					timestamp = time.Now().Format(time.RFC3339)
				}
			}

			// Process each log in the result
			for _, logEntry := range result.Logs {
				ev := types.LogEvent{
					BlockHeight:     blockHeight,
					TxHash:          txHash,
					OutputHash:      doc.DocID,
					ContractAddress: doc.ContractID,
					Log:             logEntry,
					Timestamp:       timestamp,
				}

				// Insert raw log for traceability
				_, err := db.Exec(
					`INSERT INTO contract_logs (block_height, tx_hash, contract_address, log, ts)
					 VALUES ($1, $2, $3, $4, $5)
					 ON CONFLICT DO NOTHING`,
					ev.BlockHeight, ev.TxHash, ev.ContractAddress, ev.Log, ev.Timestamp,
				)
				if err != nil {
					log.Printf("[mongo] insert contract_logs error: %v", err)
				}

				// Find mapping and insert into specific table
				if mapping := mapper.FindMapping(ev.ContractAddress, ev.Log); mapping != nil {
					datalayer.InsertLog(db, *mapping, ev)
				}

				logsProcessed++
			}
		}

		processedCount++
		if doc.BlockHeight > maxBlockHeight {
			maxBlockHeight = doc.BlockHeight
		}
	}

	if err := cursor.Err(); err != nil {
		return fmt.Errorf("cursor error: %w", err)
	}

	// Update last processed block height
	if processedCount > 0 {
		lastProcessed[contract.Address] = maxBlockHeight
		log.Printf("[mongo] processed %d documents with %d logs for %s (up to block %d)",
			processedCount, logsProcessed, contract.Address, maxBlockHeight)
	}

	return nil
}

// scanForDiscovery scans ALL contract_state documents for logs matching a DiscoverEvent.
// When found, it registers the contract_id so future polls will index its events.
func scanForDiscovery(
	ctx context.Context,
	db *sql.DB,
	contractStateCol *mongo.Collection,
	blockHeadersCol *mongo.Collection,
	txPoolCol *mongo.Collection,
	hiveBlocksCol *mongo.Collection,
	template types.ContractMapping,
	lastScan map[string]uint64,
) error {
	// Query all contract_state docs with logs, above our last scan height
	filter := bson.M{
		"block_height": bson.M{"$gt": lastScan[template.DiscoverEvent]},
		"results.logs": bson.M{"$exists": true, "$not": bson.M{"$size": 0}},
	}

	findOptions := options.Find().
		SetSort(bson.D{{Key: "block_height", Value: 1}}).
		SetLimit(1000) // limit per scan to avoid huge queries

	cursor, err := contractStateCol.Find(ctx, filter, findOptions)
	if err != nil {
		return fmt.Errorf("discovery scan query failed: %w", err)
	}
	defer cursor.Close(ctx)

	var maxBlock uint64 = lastScan[template.DiscoverEvent]

	for cursor.Next(ctx) {
		var doc MongoContractState
		if err := cursor.Decode(&doc); err != nil {
			continue
		}

		if doc.BlockHeight > maxBlock {
			maxBlock = doc.BlockHeight
		}

		// Find the event mapping for the discover event so we know the parse format
		var discoverMapping *types.EventMapping
		for i, m := range template.Events {
			if m.LogType == template.DiscoverEvent {
				discoverMapping = &template.Events[i]
				break
			}
		}
		if discoverMapping == nil {
			continue
		}

		// Check each log for the discover event
		for resultIdx, result := range doc.Results {
			for _, logEntry := range result.Logs {
				if !mapper.MatchesLogType(*discoverMapping, logEntry) {
					continue
				}

				// Found a matching discover event — register this contract
				isNew, err := mapper.RegisterDiscoveredContract(db, doc.ContractID, template.DiscoverEvent, doc.BlockHeight)
				if err != nil {
					log.Printf("[discovery] failed to register contract %s: %v", doc.ContractID, err)
					continue
				}
				if isNew {
					log.Printf("[discovery] discovered new contract %s via %q at block %d",
						doc.ContractID, template.DiscoverEvent, doc.BlockHeight)

					// Resolve input tx hash and block height
					var txHash string
					var blockHeight uint64
					if resultIdx < len(doc.Inputs) {
						txHash = doc.Inputs[resultIdx]
						var txRecord TransactionPoolRecord
						err := txPoolCol.FindOne(ctx, bson.M{"id": txHash}).Decode(&txRecord)
						if err != nil {
							log.Printf("[discovery] failed to look up tx %s in transaction_pool: %v, falling back", txHash, err)
							blockHeight = doc.BlockHeight
						} else {
							blockHeight = txRecord.AnchoredHeight
						}
					} else {
						txHash = doc.DocID
						blockHeight = doc.BlockHeight
					}

					// Get timestamp from hive_blocks, fall back to block_headers
					timestamp, err := getHiveBlockTimestamp(ctx, hiveBlocksCol, blockHeight)
					if err != nil {
						timestamp, err = getBlockTimestamp(ctx, blockHeadersCol, blockHeight)
						if err != nil {
							timestamp = time.Now().Format(time.RFC3339)
						}
					}
					ev := types.LogEvent{
						BlockHeight:     blockHeight,
						TxHash:          txHash,
						OutputHash:      doc.DocID,
						ContractAddress: doc.ContractID,
						Log:             logEntry,
						Timestamp:       timestamp,
					}

					// Insert raw log
					_, _ = db.Exec(
						`INSERT INTO contract_logs (block_height, tx_hash, contract_address, log, ts)
						 VALUES ($1, $2, $3, $4, $5)
						 ON CONFLICT DO NOTHING`,
						ev.BlockHeight, ev.TxHash, ev.ContractAddress, ev.Log, ev.Timestamp,
					)

					// Insert into mapped table if there's a mapping for the discover event
					for _, m := range template.Events {
						if m.LogType == template.DiscoverEvent {
							datalayer.InsertLog(db, m, ev)
							break
						}
					}
				}
			}
		}
	}

	if maxBlock > lastScan[template.DiscoverEvent] {
		lastScan[template.DiscoverEvent] = maxBlock
	}

	return cursor.Err()
}

// ParseMongoLog is a helper to parse log data if it comes as JSON
func ParseMongoLog(logData string) (map[string]interface{}, error) {
	var result map[string]interface{}
	if err := json.Unmarshal([]byte(logData), &result); err != nil {
		return nil, err
	}
	return result, nil
}
