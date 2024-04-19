package kinesumer

import (
	"context"
	"log"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/pkg/errors"
)

func (k *Kinesumer) loopSyncClient() {
	<-k.started

	ticker := time.NewTicker(syncInterval)
	for {
		select {
		case <-ticker.C:
			ctx := context.Background()
			ctx, cancel := context.WithTimeout(ctx, syncTimeout)

			if err := k.pingAliveness(ctx); err != nil {
				log.Println("kinesumer: failed to pingAliveness", "error", err)
			}
			if err := k.syncShardInfo(ctx); err != nil {
				log.Println("kinesumer: failed to syncShardInfo", "error", err)
			}

			if k.leader {
				if err := k.doLeadershipSyncShardIDs(ctx); err != nil {
					log.Println("kinesumer: failed to doLeadershipSyncShardIDs", "error", err)
				}
				if err := k.doLeadershipPruneClients(ctx); err != nil {
					log.Println("kinesumer: failed to doLeadershipPruneClients", "error", err)
				}
			}
			cancel()
		case <-k.close:
			ctx := context.Background()
			ctx, cancel := context.WithTimeout(ctx, syncTimeout)

			if err := k.stateStore.DeregisterClient(ctx, k.id); err != nil {
				log.Println("kinesumer: failed to DeregisterClient", "error", err)
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
	clientIDs, err := k.stateStore.ListAllAliveClientIDs(ctx)
	if err != nil {
		log.Println("failed to list alive clients", "error", err)
		return errors.WithStack(err)
	}

	log.Println("clientIDs", clientIDs)

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
			log.Println("failed to sync shard info for stream", "error", err)
			return errors.WithStack(err)
		}
	}
	return nil
}

func (k *Kinesumer) syncShardInfoForStream(
	ctx context.Context, stream string, idx, numOfClient int,
) error {
	shards, err := k.stateStore.GetShards(ctx, stream)
	if errors.Is(err, ErrNoShardCache) {
		// If there are no cache, fetch shards from Kinesis directly.
		shards, err = k.listShards(ctx, stream)
		if err != nil {
			log.Println("failed to list shards from stream", "error", err)
			return errors.WithStack(err)
		}
	} else if err != nil {
		log.Println("failed to get shards from cache", "error", err)
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

	// Sync shard check points.
	seqMap, err := k.stateStore.ListCheckPoints(ctx, stream, shardIDs)
	if err != nil {
		log.Println("failed to list check points", "error", err)
		return errors.WithStack(err)
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

	log.Println("stream", stream, "shardIds", shardIDs)

	return nil
}
