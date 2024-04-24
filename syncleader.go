package kinesumer

import (
	"context"
	"slices"
	"time"

	"github.com/pkg/errors"
)

const (
	outdatedGap = 10 * time.Second
)

func (k *Kinesumer) doLeadershipSyncShardIDs(ctx context.Context) error {
	for _, stream := range k.streams {
		shards, err := k.listShards(stream)
		if err != nil {
			return errors.WithStack(err)
		}
		if slices.Equal(k.shardCaches[stream], shards.ids()) {
			return nil
		}
		if err := k.stateStore.UpdateShards(ctx, stream, shards); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (k *Kinesumer) doLeadershipPruneClients(ctx context.Context) error {
	if err := k.stateStore.PruneClients(ctx); err != nil {
		return errors.WithStack(err)
	}
	return nil
}
