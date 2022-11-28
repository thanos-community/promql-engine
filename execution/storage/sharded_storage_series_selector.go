// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package storage

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/efficientgo/core/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

type shardedStorageSeriesSelector struct {
	storage  storage.Queryable
	mint     int64
	maxt     int64
	step     int64
	matchers []*labels.Matcher
	hints    storage.SelectHints

	once   sync.Once
	onces  []sync.Once
	shards [][]SignedSeries
}

func newShardedStorageSeriesSelector(storage storage.Queryable, mint, maxt, step int64, matchers []*labels.Matcher, hints storage.SelectHints) *shardedStorageSeriesSelector {
	return &shardedStorageSeriesSelector{
		storage:  storage,
		maxt:     maxt,
		mint:     mint,
		step:     step,
		matchers: matchers,
		hints:    hints,
	}
}

func (o *shardedStorageSeriesSelector) Explain() string {
	return fmt.Sprintf("[*shardedStorageSeriesSelector:(%p)] {%v} @%v[%v] ", o, o.matchers, o.mint, time.Millisecond*time.Duration(o.maxt-o.mint))
}

func (o *shardedStorageSeriesSelector) Matchers() []*labels.Matcher {
	return o.matchers
}

func (o *shardedStorageSeriesSelector) GetSeries(ctx context.Context, shard int, numShards int) ([]SignedSeries, error) {
	o.once.Do(func() {
		if len(o.shards) == 0 {
			o.shards = make([][]SignedSeries, numShards)
			o.onces = make([]sync.Once, numShards)
		}
	})

	var err error
	o.onces[shard].Do(func() { err = o.loadSeries(ctx, shard, numShards) })
	if err != nil {
		return nil, err
	}
	return o.shards[shard], nil
}

type shardableQuerier interface {
	SetShardInfo(shard, numShards int, by bool, labels []string)
}

func (o *shardedStorageSeriesSelector) loadSeries(ctx context.Context, shard int, numShards int) error {
	querier, err := o.storage.Querier(ctx, o.mint, o.maxt)
	if err != nil {
		return err
	}
	defer querier.Close()

	s, ok := querier.(shardableQuerier)
	if !ok {
		return errors.Newf("querier is not shardable?")
	}

	// YOLO
	s.SetShardInfo(shard, numShards, false, nil)

	seriesSet := querier.Select(false, &o.hints, o.matchers...)
	i := 0
	for seriesSet.Next() {
		s := seriesSet.At()
		o.shards[shard] = append(o.shards[shard], SignedSeries{
			Series:    s,
			Signature: uint64(i),
		})
		i++
	}

	return nil
}
