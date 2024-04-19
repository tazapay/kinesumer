package kinesumer

import (
	"fmt"
	"time"
)

var buildKeyFn = fmt.Sprintf

const (
	shardCacheKeyFmt = "shard_cache#%s"    // shard_cache#<app>.
	clientKeyFmt     = "client#%s"         // client#<app>.
	checkPointKeyFmt = "check_point#%s#%s" // check_point#<app>#<stream>.
)

// stateShardCache manages shard id list cache.
type stateShardCache struct {
	ShardCacheKey string   `dynamodbav:"pk,pk"`
	Stream        string   `dynamodbav:"sk,sk"`
	Shards        Shards   `dynamodbav:"shards"`
	ShardIDs      []string `dynamodbav:"shard_ids"` // Deprecated.
}

func buildShardCacheKey(app string) string {
	return buildKeyFn(shardCacheKeyFmt, app)
}

// stateClient manages consumer client.
type stateClient struct {
	ClientKey  string    `dynamodbav:"pk,pk"`
	ClientID   string    `dynamodbav:"sk,sk"`
	LastUpdate time.Time `dynamodbav:"last_update,lsi1"`
}

func buildClientKey(app string) string {
	return buildKeyFn(clientKeyFmt, app)
}

// ShardCheckPoint manages a shard check point.
type ShardCheckPoint struct {
	Stream         string
	ShardID        string
	SequenceNumber string
	UpdatedAt      time.Time
}

// stateCheckPoint manages record check points.
type stateCheckPoint struct {
	StreamKey      string    `dynamodbav:"pk,pk"`
	ShardID        string    `dynamodbav:"sk,sk"`
	SequenceNumber string    `dynamodbav:"sequence_number"`
	LastUpdate     time.Time `dynamodbav:"last_update"`
}

func buildCheckPointKey(app, stream string) string {
	return buildKeyFn(checkPointKeyFmt, app, stream)
}
