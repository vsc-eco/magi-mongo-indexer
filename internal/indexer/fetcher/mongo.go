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

	collection := client.Database(dbName).Collection("contract_state")

	// Track the last processed block height per contract
	lastProcessed := make(map[string]uint64)

	// Initialize last processed heights from the database
	mappings := mapper.GetMappings()
	if mappings != nil {
		for _, contract := range mappings.Contracts {
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
	}

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

			// Process each contract
			for _, contract := range mappings.Contracts {
				if err := processContract(ctx, db, collection, contract, lastProcessed); err != nil {
					log.Printf("[mongo] error processing contract %s: %v", contract.Address, err)
				}
			}
		}
	}
}

// processContract fetches new entries for a specific contract and processes them
func processContract(
	ctx context.Context,
	db *sql.DB,
	collection *mongo.Collection,
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

	cursor, err := collection.Find(ctx, filter, findOptions)
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

		// Process each result that has logs
		for _, result := range doc.Results {
			if len(result.Logs) == 0 {
				continue
			}

			// Process each log in the result
			for _, logEntry := range result.Logs {
				// Create a pseudo tx_hash from the doc ID and block height
				txHash := fmt.Sprintf("%s_%d", doc.DocID, doc.BlockHeight)

				// Convert to LogEvent
				ev := types.LogEvent{
					BlockHeight:     doc.BlockHeight,
					TxHash:          txHash,
					ContractAddress: doc.ContractID,
					Log:             logEntry,
					Timestamp:       time.Now().Format(time.RFC3339), // We don't have timestamp in the doc
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
					continue
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

// ParseMongoLog is a helper to parse log data if it comes as JSON
func ParseMongoLog(logData string) (map[string]interface{}, error) {
	var result map[string]interface{}
	if err := json.Unmarshal([]byte(logData), &result); err != nil {
		return nil, err
	}
	return result, nil
}
