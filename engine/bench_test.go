// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package engine_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-community/promql-engine/engine"
)

func BenchmarkChunkDecoding(b *testing.B) {
	test := setupStorage(b, 1000, 3)
	defer test.Close()

	start := time.Unix(0, 0)
	end := start.Add(6 * time.Hour)
	step := time.Second * 30

	querier, err := test.Storage().Querier(test.Context(), start.UnixMilli(), end.UnixMilli())
	testutil.Ok(b, err)

	matcher, err := labels.NewMatcher(labels.MatchEqual, labels.MetricName, "http_requests_total")
	testutil.Ok(b, err)

	b.Run("iterate by series", func(b *testing.B) {
		b.ResetTimer()
		for c := 0; c < b.N; c++ {
			numIterations := 0

			ss := querier.Select(false, nil, matcher)
			series := make([]chunkenc.Iterator, 0)
			for ss.Next() {
				series = append(series, ss.At().Iterator())
			}
			for i := 0; i < len(series); i++ {
				for ts := start.UnixMilli(); ts <= end.UnixMilli(); ts += step.Milliseconds() {
					numIterations++
					if ok := series[i].Seek(ts); !ok {
						break
					}
				}
			}
		}
	})
	b.Run("iterate by time", func(b *testing.B) {
		b.ResetTimer()
		for c := 0; c < b.N; c++ {
			numIterations := 0
			ss := querier.Select(false, nil, matcher)
			series := make([]chunkenc.Iterator, 0)
			for ss.Next() {
				series = append(series, ss.At().Iterator())
			}
			stepCount := 10
			ts := start.UnixMilli()
			for ts <= end.UnixMilli() {
				for i := 0; i < len(series); i++ {
					seriesTs := ts
					for currStep := 0; currStep < stepCount && seriesTs <= end.UnixMilli(); currStep++ {
						numIterations++
						if ok := series[i].Seek(seriesTs); !ok {
							break
						}
						seriesTs += step.Milliseconds()
					}
				}
				ts += step.Milliseconds() * int64(stepCount)
			}
		}
	})
}

func BenchmarkSingleQuery(b *testing.B) {
	test := setupStorage(b, 5000, 3)
	defer test.Close()

	start := time.Unix(0, 0)
	end := start.Add(6 * time.Hour)
	step := time.Second * 30

	query := "sum(http_requests_total)"
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		executeQuery(b, query, test, start, end, step)
	}
}

func BenchmarkOldEngine(b *testing.B) {
	test := setupStorage(b, 1000, 3)
	defer test.Close()

	start := time.Unix(0, 0)
	end := start.Add(2 * time.Hour)
	step := time.Second * 30

	cases := []struct {
		name  string
		query string
	}{
		{
			name:  "vector selector",
			query: "http_requests_total",
		},
		{
			name:  "sum",
			query: "sum(http_requests_total)",
		},
		{
			name:  "sum by pod",
			query: "sum by (pod) (http_requests_total)",
		},
		{
			name:  "rate",
			query: "rate(http_requests_total[1m])",
		},
		{
			name:  "sum rate",
			query: "sum(rate(http_requests_total[1m]))",
		},
		{
			name:  "sum by rate",
			query: "sum by (pod) (rate(http_requests_total[1m]))",
		},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.Run("current_engine", func(b *testing.B) {
				opts := promql.EngineOpts{
					Logger:     nil,
					Reg:        nil,
					MaxSamples: 50000000,
					Timeout:    100 * time.Second,
				}
				engine := promql.NewEngine(opts)

				b.ResetTimer()
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					qry, err := engine.NewRangeQuery(test.Queryable(), nil, tc.query, start, end, step)
					testutil.Ok(b, err)

					res := qry.Exec(test.Context())
					testutil.Ok(b, res.Err)
				}
			})
			b.Run("new_engine", func(b *testing.B) {
				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					executeQuery(b, tc.query, test, start, end, step)
				}
			})
		})
	}
}

func executeQuery(b *testing.B, q string, test *promql.Test, start time.Time, end time.Time, step time.Duration) {
	ng := engine.New(engine.Opts{DisableFallback: true})
	qry, err := ng.NewRangeQuery(test.Queryable(), nil, q, start, end, step)
	testutil.Ok(b, err)

	qry.Exec(context.Background())
}

func setupStorage(b *testing.B, numLabelsA int, numLabelsB int) *promql.Test {
	load := synthesizeLoad(numLabelsA, numLabelsB)
	test, err := promql.NewTest(b, load)
	testutil.Ok(b, err)
	testutil.Ok(b, test.Run())

	return test
}

func synthesizeLoad(numPods, numContainers int) string {
	load := `
load 30s`
	for i := 0; i < numPods; i++ {
		for j := 0; j < numContainers; j++ {
			load += fmt.Sprintf(`
  http_requests_total{pod="p%d", container="c%d"} %d+%dx720`, i, j, i, j)
		}
	}

	return load
}
