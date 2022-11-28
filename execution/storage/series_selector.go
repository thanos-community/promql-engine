// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package storage

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

type SeriesSelector interface {
	GetSeries(ctx context.Context, shard, numShards int) ([]SignedSeries, error)
	Matchers() []*labels.Matcher
	Explain() string
}

type SignedSeries struct {
	storage.Series
	Signature uint64
}

type seriesSelector struct {
	storage  storage.Queryable
	mint     int64
	maxt     int64
	step     int64
	matchers []*labels.Matcher
	hints    storage.SelectHints

	once   sync.Once
	series []SignedSeries
}

func newSeriesSelector(storage storage.Queryable, mint, maxt, step int64, matchers []*labels.Matcher, hints storage.SelectHints) *seriesSelector {
	return &seriesSelector{
		storage:  storage,
		maxt:     maxt,
		mint:     mint,
		step:     step,
		matchers: matchers,
		hints:    hints,
	}
}

func (o *seriesSelector) Explain() string {
	return fmt.Sprintf("[*seriesSelector:(%p)] {%v} @%v[%v] ", o, o.matchers, o.mint, time.Millisecond*time.Duration(o.maxt-o.mint))
}

func (o *seriesSelector) Matchers() []*labels.Matcher {
	return o.matchers
}

func (o *seriesSelector) GetSeries(ctx context.Context, shard int, numShards int) ([]SignedSeries, error) {
	var err error
	o.once.Do(func() { err = o.loadSeries(ctx) })
	if err != nil {
		return nil, err
	}

	return seriesShard(o.series, shard, numShards), nil
}

func (o *seriesSelector) loadSeries(ctx context.Context) error {
	querier, err := o.storage.Querier(ctx, o.mint, o.maxt)
	if err != nil {
		return err
	}
	defer querier.Close()

	seriesSet := querier.Select(false, &o.hints, o.matchers...)
	i := 0
	for seriesSet.Next() {
		s := seriesSet.At()
		o.series = append(o.series, SignedSeries{
			Series:    s,
			Signature: uint64(i),
		})
		i++
	}

	return seriesSet.Err()
}

func seriesShard(series []SignedSeries, shard int, numShards int) []SignedSeries {
	start := shard * len(series) / numShards
	end := (shard + 1) * len(series) / numShards
	if end > len(series) {
		end = len(series)
	}
	return series[start:end]
}
