package kinesumer

import (
	"context"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sethvargo/go-retry"
	"github.com/tazapay/grpc-framework/logger"
)

// backoff config (extended values, since connection creation has part of service uptime)
const (
	baseDelay     = 100 * time.Millisecond
	maxDuration   = 1 * time.Minute
	maxRetries    = 3
	jitterPercent = 30 // between 25 and 50
)

func (k *Kinesumer) loopSyncClient() {
	log := logger.FromContext(context.Background())
	<-k.started

	ticker := time.NewTicker(syncInterval)
	for {
		select {
		case <-ticker.C:
			ctx := context.Background()
			ctx, cancel := context.WithTimeout(ctx, syncTimeout)

			if err := k.pingAliveness(ctx); err != nil {
				log.Error("kinesumer: failed to pingAliveness", err)
			}
			if err := k.syncShardInfo(ctx); err != nil {
				log.Error("kinesumer: failed to syncShardInfo", err)
			}

			if k.leader {
				if err := k.doLeadershipSyncShardIDs(ctx); err != nil {
					log.Error("kinesumer: failed to doLeadershipSyncShardIDs", err)
				}
				if err := k.doLeadershipPruneClients(ctx); err != nil {
					log.Error("kinesumer: failed to doLeadershipPruneClients", err)
				}
			}
			cancel()
		case <-k.close:
			ctx := context.Background()
			ctx, cancel := context.WithTimeout(ctx, syncTimeout)

			if err := k.stateStore.DeregisterClient(ctx, k.id); err != nil {
				log.Error("kinesumer: failed to DeregisterClient", err)
			}
			cancel()
			return
		}
	}
}

func (k *Kinesumer) pingAliveness(ctx context.Context) error {
	if err := k.stateStore.PingClientAliveness(ctx, k.id); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (k *Kinesumer) syncShardInfo(ctx context.Context) error {
	log := logger.FromContext(ctx)

	clientIDs, err := k.stateStore.ListAllAliveClientIDs(ctx)
	if err != nil {
		log.Error("failed to list alive clients", err)
		return errors.WithStack(err)
	}

	log.Info("syncing shard info", "clientIDs", clientIDs)

	// Skip if there are no alive clients.
	numOfClient := len(clientIDs)
	if numOfClient == 0 {
		return nil
	}

	// Simple leader selection: take first (order by client id).
	var idx int
	for i, id := range clientIDs {
		if id == k.id {
			idx = i
			break
		}
	}
	k.leader = idx == 0

	// Update shard information.
	for _, stream := range k.streams {
		if err := k.syncShardInfoForStream(ctx, stream, idx, numOfClient); err != nil {
			log.Error("failed to sync shard info for stream", err)
			return errors.WithStack(err)
		}
	}
	return nil
}

func (k *Kinesumer) syncShardInfoForStream(
	ctx context.Context, stream string, idx, numOfClient int,
) error {
	log := logger.FromContext(ctx)

	shards, err := k.stateStore.GetShards(ctx, stream)
	if errors.Is(err, ErrNoShardCache) {
		// If there are no cache, fetch shards from Kinesis directly.
		shards, err = k.listShards(stream)
		if err != nil {
			log.Error("failed to list shards from stream", err)
			return errors.WithStack(err)
		}
	} else if err != nil {
		log.Error("failed to get shards from cache", err)
		return errors.WithStack(err)
	}

	numShards := len(shards)

	// Assign a partial range of shard list to client.
	r := float64(numShards) / float64(numOfClient)
	splitStartIdx := int(math.Round(float64(idx) * r))
	splitEndIdx := int(math.Round(float64(idx+1) * r))
	newShards := shards[splitStartIdx:splitEndIdx]

	if slices.Equal(k.shards[stream].ids(), newShards.ids()) {
		return nil
	}

	k.mu.Lock()
	defer k.mu.Unlock()

	// Update client shard ids.
	k.pause() // Pause the current consuming jobs before update shards.
	k.shards[stream] = newShards
	defer k.start() // Re-start the consuming jobs with updated shards.

	// Sync next iterators map.
	if _, ok := k.nextIters[stream]; !ok {
		k.nextIters[stream] = &sync.Map{}
	}

	// Delete uninterested shard ids.
	shardIDs := k.shards[stream].ids()
	k.nextIters[stream].Range(func(key, _ interface{}) bool {
		if !slices.Contains(shardIDs, key.(string)) {
			k.nextIters[stream].Delete(key)
		}

		return true
	})

	var seqMap map[string]string

	retry.Do(ctx, func() retry.Backoff {
		r := retry.NewFibonacci(baseDelay)            // start with a small delay to avoid overloading the server
		r = retry.WithJitterPercent(jitterPercent, r) // jitter to the backoff delays to avoid "thundering herd"
		r = retry.WithMaxRetries(maxRetries, r)       // terminate a retry after Nth attempt

		return r
	}(),
		func(ctx context.Context) error {
			// Sync shard check points.
			seqMap, err = k.stateStore.ListCheckPoints(ctx, stream, shardIDs)
			if err != nil {
				log.Error("failed to list check points", err)
				return errors.WithStack(err)
			}

			if len(seqMap) == 0 || (len(seqMap) == 1 && func() bool {
				_, ok := seqMap[""]
				return ok
			}()) {
				log.Info("Checkpoint found to be empty, retrying...")
				return retry.RetryableError(ErrEmptySequenceNumber)
			}

			return nil
		})

	if len(seqMap) == 0 || (len(seqMap) == 1 && func() bool {
		_, ok := seqMap[""]
		return ok
	}()) {
		log.Debug("no checkpoints or empty checkpoints present in the map,"+
			" might use TRIM_HORIZON or AT_TIMESTAMP config",
			"stream", stream, "shardIDs", shardIDs)
	} else {
		log.Debug("found checkpoints for the stream", "stream",
			stream, "count", len(seqMap), "checkpoints", seqMap)
	}

	if _, ok := k.checkPoints[stream]; !ok {
		k.checkPoints[stream] = &sync.Map{}
	}
	if _, ok := k.offsets[stream]; !ok {
		k.offsets[stream] = &sync.Map{}
	}

	// Delete uninterested shard ids.
	k.checkPoints[stream].Range(func(key, _ interface{}) bool {
		if _, ok := seqMap[key.(string)]; !ok {
			k.checkPoints[stream].Delete(key)
		}
		return true
	})
	for id, seq := range seqMap {
		k.checkPoints[stream].Store(id, seq)
	}

	log.Debug("shard info for stream data", "stream", stream, "shardIds", shardIDs)

	return nil
}
