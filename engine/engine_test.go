// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package engine_test

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"go.uber.org/goleak"

	"github.com/thanos-community/promql-engine/engine"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestVectorSelectorWithGaps(t *testing.T) {
	opts := promql.EngineOpts{
		Timeout:              1 * time.Hour,
		MaxSamples:           1e10,
		EnableNegativeOffset: true,
		EnableAtModifier:     true,
	}

	series := storage.MockSeries(
		[]int64{240, 270, 300, 600, 630, 660},
		[]float64{1, 2, 3, 4, 5, 6},
		[]string{labels.MetricName, "foo"},
	)

	query := "foo"
	start := time.Unix(0, 0)
	end := time.Unix(1000, 0)

	newEngine := engine.New(engine.Opts{EngineOpts: opts})
	q1, err := newEngine.NewRangeQuery(storageWithSeries(series), nil, query, start, end, 30*time.Second)
	testutil.Ok(t, err)
	defer q1.Close()

	newResult := q1.Exec(context.Background())
	testutil.Ok(t, newResult.Err)

	oldEngine := promql.NewEngine(opts)
	q2, err := oldEngine.NewRangeQuery(storageWithSeries(series), nil, query, start, end, 30*time.Second)
	testutil.Ok(t, err)
	defer q2.Close()

	oldResult := q2.Exec(context.Background())
	testutil.Ok(t, oldResult.Err)

	testutil.Equals(t, oldResult, newResult)

}

func TestQueriesAgainstOldEngine(t *testing.T) {
	start := time.Unix(0, 0)
	end := time.Unix(240, 0)
	step := time.Second * 30
	// Negative offset and at modifier are enabled by default
	// since Prometheus v2.33.0 so we also enable them.
	opts := promql.EngineOpts{
		Timeout:              1 * time.Hour,
		MaxSamples:           1e10,
		EnableNegativeOffset: true,
		EnableAtModifier:     true,
	}

	cases := []struct {
		load  string
		name  string
		query string
		start time.Time
		end   time.Time
		step  time.Duration
	}{
		{
			name: "func with scalar arg that selects storage, checks whether same series handled correctly",
			load: `load 30s
			    thanos_cache_redis_hits_total{name="caching-bucket",service="thanos-store"} 1+1x30`,
			query: `clamp_min(thanos_cache_redis_hits_total, scalar(max(thanos_cache_redis_hits_total) by (service))) + clamp_min(thanos_cache_redis_hits_total, scalar(max(thanos_cache_redis_hits_total) by (service)))`,
		},
		{
			name: "sum + rate divided by itself",
			load: `load 30s
			thanos_cache_redis_hits_total{name="caching-bucket",service="thanos-store"} 1+1x30`,
			query: `(sum(rate(thanos_cache_redis_hits_total{name="caching-bucket"}[2m])) by (service)) /
			(sum(rate(thanos_cache_redis_hits_total{name="caching-bucket"}[2m])) by (service))`,
		},
		{
			name: "stddev_over_time",
			load: `load 30s
					http_requests_total{pod="nginx-1"} 1+1x15
					http_requests_total{pod="nginx-2"} 1+2x18`,
			query: "stddev_over_time(http_requests_total[30s])",
		},
		{
			name: "stdvar_over_time",
			load: `load 30s
					http_requests_total{pod="nginx-1"} 1+1x15
					http_requests_total{pod="nginx-2"} 1+2x18`,
			query: "stdvar_over_time(http_requests_total[30s])",
		},
		{
			name: "changes",
			load: `load 30s
					http_requests_total{pod="nginx-1"} 1+1x15
					http_requests_total{pod="nginx-2"} 1+2x18
					http_requests_total{pod="nginx-2"} 1+2x18
					http_requests_total{pod="nginx-2"} 1+2x18
					http_requests_total{pod="nginx-2"} 1+2x18`,
			query: "changes(http_requests_total[30s])",
		},
		{
			name: "deriv",
			load: `load 30s
					http_requests_total{pod="nginx-1"} 1+1x15
					http_requests_total{pod="nginx-2"} 1+2x18
					http_requests_total{pod="nginx-2"} 1+2x18
					http_requests_total{pod="nginx-2"} 1+2x18
					http_requests_total{pod="nginx-2"} 1+2x18`,
			query: "deriv(http_requests_total[30s])",
		},
		{
			name: "sum",
			load: `load 30s
					http_requests_total{pod="nginx-1"} 1+1x15
					http_requests_total{pod="nginx-2"} 1+2x18`,
			query: "sum (http_requests_total)",
		},
		{
			name: "sum_over_time",
			load: `load 30s
					http_requests_total{pod="nginx-1"} 1+1x15
					http_requests_total{pod="nginx-2"} 1+2x18`,
			query: "sum_over_time(http_requests_total[30s])",
		},
		{
			name: "count",
			load: `load 30s
					http_requests_total{pod="nginx-1"} 1+1x15
					http_requests_total{pod="nginx-2"} 1+2x18`,
			query: "count(http_requests_total)",
		},
		{
			name: "count_over_time",
			load: `load 30s
					http_requests_total{pod="nginx-1"} 1+1x15
					http_requests_total{pod="nginx-2"} 1+2x18`,
			query: "count_over_time(http_requests_total[30s])",
		},
		{
			name: "average",
			load: `load 30s
					http_requests_total{pod="nginx-1"} 1+1x15
					http_requests_total{pod="nginx-2"} 1+2x18`,
			query: "avg(http_requests_total)",
		},
		{
			name: "avg_over_time",
			load: `load 30s
					http_requests_total{pod="nginx-1"} 1+1x15
					http_requests_total{pod="nginx-2"} 1+2x18`,
			query: "avg_over_time(http_requests_total[30s])",
		},
		{
			name: "max",
			load: `load 30s
					http_requests_total{pod="nginx-1"} 1+1x15
					http_requests_total{pod="nginx-2"} 1+2x18`,
			query: "max(http_requests_total)",
		},
		{
			name: "max with only 1 sample",
			load: `load 30s
					http_requests_total{pod="nginx-1"} -1
					http_requests_total{pod="nginx-2"} 1`,
			query: "max(http_requests_total) by (pod)",
		},
		{
			name: "max_over_time",
			load: `load 30s
					http_requests_total{pod="nginx-1"} 1+1x15
					http_requests_total{pod="nginx-2"} 1+2x18`,
			query: "max_over_time(http_requests_total[30s])",
		},
		{
			name: "min",
			load: `load 30s
					http_requests_total{pod="nginx-1"} 1+1x15
					http_requests_total{pod="nginx-2"} 1+2x18`,
			query: "min(http_requests_total)",
		},
		{
			name: "min with only 1 sample",
			load: `load 30s
					http_requests_total{pod="nginx-1"} -1
					http_requests_total{pod="nginx-2"} 1`,
			query: "min(http_requests_total) by (pod)",
		},
		{
			name: "min_over_time",
			load: `load 30s
					http_requests_total{pod="nginx-1"} 1+1x15
					http_requests_total{pod="nginx-2"} 1+2x18`,
			query: "min_over_time(http_requests_total[30s])",
		},
		{
			name: "count_over_time",
			load: `load 30s
					http_requests_total{pod="nginx-1"} 1+1x15
					http_requests_total{pod="nginx-2"} 1+2x18`,
			query: "count_over_time(http_requests_total[30s])",
		},
		{
			name: "sum by pod",
			load: `load 30s
					http_requests_total{pod="nginx-1"} 1+1x15
					http_requests_total{pod="nginx-2"} 1+2x18
					http_requests_total{pod="nginx-3"} 1+2x20
					http_requests_total{pod="nginx-4"} 1+2x50`,
			query: "sum by (pod) (http_requests_total)",
		},
		{
			name: "multi label grouping by",
			load: `load 30s
					http_requests_total{pod="nginx-1", ns="a"} 1+1x15
					http_requests_total{pod="nginx-2", ns="a"} 1+1x15`,
			query: `avg by (pod, ns) (avg_over_time(http_requests_total[2m]))`,
		},
		{
			name: "multi label grouping without",
			load: `load 30s
					http_requests_total{pod="nginx-1", ns="a"} 1+1x15
					http_requests_total{pod="nginx-2", ns="a"} 1+1x15`,
			query: `avg without (pod, ns) (avg_over_time(http_requests_total[2m]))`,
		},
		{
			name: "query in the future",
			load: `load 30s
					http_requests_total{pod="nginx-1"} 1+1x15
					http_requests_total{pod="nginx-2"} 1+2x18`,
			query: "sum by (pod) (http_requests_total)",
			start: time.Unix(400, 0),
			end:   time.Unix(3000, 0),
		},
		{
			name: "count_over_time",
			load: `load 30s
					http_requests_total{pod="nginx-1"} 1
					http_requests_total{pod="nginx-1"} 1+1x30
					http_requests_total{pod="nginx-2"} 1+2x600`,
			query: `count_over_time(http_requests_total[10m])`,
			start: time.Unix(60, 0),
			end:   time.Unix(600, 0),
		},
		{
			name: "rate",
			load: `load 30s
				http_requests_total{pod="nginx-1", series="1"} 1+1.1x40
				http_requests_total{pod="nginx-2", series="2"} 2+2.3x50
				http_requests_total{pod="nginx-4", series="3"} 5+2.4x50
				http_requests_total{pod="nginx-5", series="1"} 8.4+2.3x50
				http_requests_total{pod="nginx-6", series="2"} 2.3+2.3x50`,
			query: "rate(http_requests_total[1m])",
			start: time.Unix(0, 0),
			end:   time.Unix(3000, 0),
			step:  2 * time.Second,
		},
		{
			name: "sum rate",
			load: `load 30s
					http_requests_total{pod="nginx-1"} 1+1x4
					http_requests_total{pod="nginx-2"} 1+2x4`,
			query: "sum(rate(http_requests_total[1m]))",
		},
		{
			name: "sum rate with stale series",
			load: `load 30s
					http_requests_total{pod="nginx-1"} 1+1x40
					http_requests_total{pod="nginx-2"} 1+2x50
					http_requests_total{pod="nginx-4"} 1+2x50
					http_requests_total{pod="nginx-5"} 1+2x50
					http_requests_total{pod="nginx-6"} 1+2x50`,
			query: "sum(rate(http_requests_total[1m]))",
			start: time.Unix(421, 0),
			end:   time.Unix(3230, 0),
			step:  28 * time.Second,
		},
		{
			name: "delta",
			load: `load 30s
				http_requests_total{pod="nginx-1", series="1"} 1+1.1x40
				http_requests_total{pod="nginx-2", series="2"} 2+2.3x50
				http_requests_total{pod="nginx-4", series="3"} 5+2.4x50
				http_requests_total{pod="nginx-5", series="1"} 8.4+2.3x50
				http_requests_total{pod="nginx-6", series="2"} 2.3+2.3x50`,
			query: "delta(http_requests_total[1m])",
			start: time.Unix(0, 0),
			end:   time.Unix(3000, 0),
			step:  2 * time.Second,
		},
		{
			name: "increase",
			load: `load 30s
				http_requests_total{pod="nginx-1", series="1"} 1+1.1x40
				http_requests_total{pod="nginx-2", series="2"} 2+2.3x50
				http_requests_total{pod="nginx-4", series="3"} 5+2.4x50
				http_requests_total{pod="nginx-5", series="1"} 8.4+2.3x50
				http_requests_total{pod="nginx-6", series="2"} 2.3+2.3x50`,
			query: "increase(http_requests_total[1m])",
			start: time.Unix(0, 0),
			end:   time.Unix(3000, 0),
			step:  2 * time.Second,
		},
		{
			name: "irate",
			load: `load 30s
				http_requests_total{pod="nginx-1", series="1"} 1+1.1x40
				http_requests_total{pod="nginx-2", series="2"} 2+2.3x50
				http_requests_total{pod="nginx-4", series="3"} 5+2.4x50
				http_requests_total{pod="nginx-5", series="1"} 8.4+2.3x50
				http_requests_total{pod="nginx-6", series="2"} 2.3+2.3x50`,
			query: "irate(http_requests_total[1m])",
			start: time.Unix(0, 0),
			end:   time.Unix(3000, 0),
			step:  2 * time.Second,
		},
		{
			name: "idelta",
			load: `load 30s
				http_requests_total{pod="nginx-1", series="1"} 1+1.1x40
				http_requests_total{pod="nginx-2", series="2"} 2+2.3x50
				http_requests_total{pod="nginx-4", series="3"} 5+2.4x50
				http_requests_total{pod="nginx-5", series="1"} 8.4+2.3x50
				http_requests_total{pod="nginx-6", series="2"} 2.3+2.3x50`,
			query: "idelta(http_requests_total[1m])",
			start: time.Unix(0, 0),
			end:   time.Unix(3000, 0),
			step:  2 * time.Second,
		},
		{
			name:  "number literal",
			load:  "",
			query: "34",
		},
		{
			name:  "vector",
			load:  "",
			query: "vector(24)",
		},
		{
			name: "binary operation with one-to-one matching",
			load: `load 30s
				foo{method="get", code="500"} 1+1x1
				foo{method="get", code="404"} 2+1x2
				foo{method="put", code="501"} 3+1x3
				foo{method="put", code="500"} 1+1x4
				foo{method="post", code="500"} 4+1x4
				foo{method="post", code="404"} 5+1x5
				bar{method="get"} 1+1x1
				bar{method="del"} 2+1x2
				bar{method="post"} 3+1x3`,
			query: `foo{code="500"} + ignoring(code) bar`,
			start: time.Unix(0, 0),
			end:   time.Unix(600, 0),
		},
		{
			// Example from https://prometheus.io/docs/prometheus/latest/querying/operators/#many-to-one-and-one-to-many-vector-matches
			name: "binary operation with group_left",
			load: `load 30s
				foo{method="get", code="500", path="/"} 1+1.1x30
				foo{method="get", code="404", path="/"} 1+2.2x20
				foo{method="put", code="501", path="/"} 4+3.4x60
				foo{method="post", code="500", path="/"} 1+5.1x40
				foo{method="post", code="404", path="/"} 2+3.7x40
				bar{method="get", path="/a"} 3+7.4x10
				bar{method="del", path="/b"} 8+6.1x30
				bar{method="post", path="/c"} 1+2.1x40`,
			query: `foo * ignoring(path, code) group_left bar`,
			start: time.Unix(0, 0),
			end:   time.Unix(600, 0),
		},
		{
			// Example from https://prometheus.io/docs/prometheus/latest/querying/operators/#many-to-one-and-one-to-many-vector-matches
			name: "binary operation with group_right",
			load: `load 30s
				foo{method="get", code="500"} 1+1.1x30
				foo{method="get", code="404"} 1+2.2x20
				foo{method="put", code="501"} 4+3.4x60
				foo{method="post", code="500"} 1+5.1x40
				foo{method="post", code="404"} 2+3.7x40
				bar{method="get", path="/a"} 3+7.4x10
				bar{method="del", path="/b"} 8+6.1x30
				bar{method="post", path="/c"} 1+2.1x40`,
			query: `bar * ignoring(code, path) group_right foo`,
			start: time.Unix(0, 0),
			end:   time.Unix(3000, 0),
			step:  2 * time.Second,
		},
		{
			name: "binary operation with group_left and included labels",
			load: `load 30s
				foo{method="get", code="500"} 1+1.1x30
				foo{method="get", code="404"} 1+2.2x20
				foo{method="put", code="501"} 4+3.4x60
				foo{method="post", code="500"} 1+5.1x40
				foo{method="post", code="404"} 2+3.7x40
				bar{method="get", path="/a"} 3+7.4x10
				bar{method="del", path="/b"} 8+6.1x30
				bar{method="post", path="/c"} 1+2.1x40`,
			query: `foo * ignoring(code, path) group_left(path) bar`,
			start: time.Unix(0, 0),
			end:   time.Unix(600, 0),
		},
		{
			name: "binary operation with group_right and included labels",
			load: `load 30s
				foo{method="get", code="500"} 1+1.1x30
				foo{method="get", code="404"} 1+2.2x20
				foo{method="put", code="501"} 4+3.4x60
				foo{method="post", code="500"} 1+5.1x40
				foo{method="post", code="404"} 2+3.7x40
				bar{method="get", path="/a"} 3+7.4x10
				bar{method="del", path="/b"} 8+6.1x30
				bar{method="post", path="/c"} 1+2.1x40`,
			query: `bar * ignoring(code, path) group_right(path) foo`,
			start: time.Unix(0, 0),
			end:   time.Unix(600, 0),
		},
		{
			name: "binary operation with vector and scalar on the right",
			load: `load 30s
				foo{method="get", code="500"} 1+1.1x30
				foo{method="get", code="404"} 1+2.2x20`,
			query: `sum(foo) * 2`,
		},
		{
			name: "binary operation with vector and scalar on the left",
			load: `load 30s
				foo{method="get", code="500"} 1+1.1x30
				foo{method="get", code="404"} 1+2.2x20`,
			query: `2 * sum(foo)`,
		},
		{
			name: "complex binary operation",
			load: `load 30s
				foo{method="get", code="500"} 1+1.1x30
				foo{method="get", code="404"} 1+2.2x20`,
			query: `1 - (100 * sum(foo{method="get"}) / sum(foo))`,
		},
		{
			name: "vector binary op ==",
			load: `load 30s
				foo{method="get", code="500"} 1+1x40
				bar{method="get", code="404"} 1+1.1x30`,
			query: `sum(foo) by (method) == sum(bar) by (method)`,
		},
		{
			name: "vector binary op !=",
			load: `load 30s
				foo{method="get", code="500"} 1+1x40
				bar{method="get", code="404"} 1+1.1x30`,
			query: `sum(foo) by (method) != sum(bar) by (method)`,
		},
		{
			name: "vector binary op >",
			load: `load 30s
				foo{method="get", code="500"} 1+1x40
				bar{method="get", code="404"} 1+1.1x30`,
			query: `sum(foo) by (method) > sum(bar) by (method)`,
		},
		{
			name: "vector binary op with name <",
			load: `load 30s
				foo{method="get", code="500"} 1+1x40
				bar{method="get", code="500"} 1+1.1x30`,
			query: `foo < bar`,
		},
		{
			name: "vector binary op with name < scalar",
			load: `load 30s
				foo{method="get", code="500"} 1+1x40
				bar{method="get", code="500"} 1+1.1x30`,
			query: `foo < 10`,
		},
		{
			name: "vector binary op > scalar",
			load: `load 30s
				foo{method="get", code="500"} 1+2x40
				bar{method="get", code="404"} 1+1x30`,
			query: `sum(foo) by (method) > 10`,
		},
		{
			name: "scalar < vector binary op",
			load: `load 30s
				foo{method="get", code="500"} 1+2x40
				bar{method="get", code="404"} 1+1x30`,
			query: `10 < sum(foo) by (method)`,
		},
		{
			name: "vector binary op <",
			load: `load 30s
				foo{method="get", code="500"} 1+1x40
				bar{method="get", code="404"} 1+1.1x30`,
			query: `sum(foo) by (method) < sum(bar) by (method)`,
		},
		{
			name: "vector binary op >=",
			load: `load 30s
				foo{method="get", code="500"} 1+1x40
				bar{method="get", code="404"} 1+1.1x30`,
			query: `sum(foo) by (method) >= sum(bar) by (method)`,
		},
		{
			name: "vector binary op <=",
			load: `load 30s
				foo{method="get", code="500"} 1+1x40
				bar{method="get", code="404"} 1+1.1x30`,
			query: `sum(foo) by (method) <= sum(bar) by (method)`,
		},
		{
			name: "vector binary op ^",
			load: `load 30s
				foo{method="get", code="500"} 1+1x40
				bar{method="get", code="404"} 1+1.1x30`,
			query: `sum(foo) by (method) ^ sum(bar) by (method)`,
		},
		{
			name: "vector binary op %",
			load: `load 30s
				foo{method="get", code="500"} 1+2x40
				bar{method="get", code="404"} 1+1x30`,
			query: `sum(foo) by (method) % sum(bar) by (method)`,
		},
		{
			name:  "scalar binary op == true",
			load:  ``,
			query: `1 == bool 1`,
		},
		{
			name:  "scalar binary op == false",
			load:  ``,
			query: `1 != bool 2`,
		},
		{
			name:  "scalar binary op !=",
			load:  ``,
			query: `1 != bool 1`,
		},
		{
			name:  "scalar binary op >",
			load:  ``,
			query: `1 > bool 0`,
		},
		{
			name:  "scalar binary op <",
			load:  ``,
			query: `1 > bool 2`,
		},
		{
			name:  "scalar binary op >=",
			load:  ``,
			query: `1 >= bool 0`,
		},
		{
			name:  "scalar binary op <=",
			load:  ``,
			query: `1 <= bool 2`,
		},
		{
			name:  "scalar binary op % 0",
			load:  ``,
			query: `2 % 2`,
		},
		{
			name:  "scalar binary op % 1",
			load:  ``,
			query: `1 % 2`,
		},
		{
			name:  "scalar binary op ^",
			load:  ``,
			query: `2 ^ 2`,
		},
		{
			name:  "empty series",
			load:  "",
			query: "http_requests_total",
		},
		{
			name:  "empty series with func",
			load:  "",
			query: "sum(http_requests_total)",
		},
		{
			name: "empty result",
			load: `load 30s
					http_requests_total{pod="nginx-1"} 1+1x15
					http_requests_total{pod="nginx-2"} 1+2x18`,
			query: `http_requests_total{pod="nginx-3"}`,
		},
		{
			name: "last_over_time",
			load: `load 30s
					http_requests_total{pod="nginx-1"} 1+1x15
					http_requests_total{pod="nginx-2"} 1+2x18`,
			query: `last_over_time(http_requests_total[30s])`,
		},
		{
			name: "group",
			load: `load 30s
					http_requests_total{pod="nginx-1"} 1+1x15
					http_requests_total{pod="nginx-2"} 1+2x18`,
			query: `group(http_requests_total)`,
		},
		{
			name: "resets",
			load: `load 30s
					http_requests_total{pod="nginx-1"} 100-1x15
					http_requests_total{pod="nginx-2"} 1+2x18`,
			query: `resets(http_requests_total[5m])`,
		},
		{
			name: "present_over_time",
			load: `load 30s
					http_requests_total{pod="nginx-1"} 1+1x15
					http_requests_total{pod="nginx-2"} 1+2x18`,
			query: `present_over_time(http_requests_total[30s])`,
		},
		{
			name: "complex binary with aggregation",
			load: `load 30s
					grpc_server_handled_total{pod="nginx-1", grpc_method="Series", grpc_code="105"} 1+1x15
					grpc_server_handled_total{pod="nginx-2", grpc_method="Series", grpc_code="105"} 1+1x15
					grpc_server_handled_total{pod="nginx-3", grpc_method="Series", grpc_code="105"} 1+1x15
					prometheus_tsdb_head_samples_appended_total{pod="nginx-1", tenant="tenant-1"} 1+2x18
					prometheus_tsdb_head_samples_appended_total{pod="nginx-2", tenant="tenant-2"} 1+2x18
					prometheus_tsdb_head_samples_appended_total{pod="nginx-3", tenant="tenant-3"} 1+2x18`,
			query: `
	sum by (grpc_method, grpc_code) (
		sum by (pod, grpc_method, grpc_code) (
			rate(grpc_server_handled_total{grpc_method="Series", pod=~".+"}[1m])
		)
		+ on (pod) group_left() max by (pod) (
			prometheus_tsdb_head_samples_appended_total{pod=~".+"}
		)
	)`,
		},
		{
			name:  "unary sub operation for scalar",
			load:  ``,
			query: `-(1 + 5)`,
		},
		{
			name: "unary sub operation for vector",
			load: `load 30s
					http_requests_total{pod="nginx-1"} 1+1x15
					http_requests_total{pod="nginx-2"} 1+2x18`,
			query: `-http_requests_total`,
		},
		{
			name: "unary add operation for vector",
			load: `load 30s
					http_requests_total{pod="nginx-1"} 1+1x15
					http_requests_total{pod="nginx-2"} 1+2x18`,
			query: `+http_requests_total`,
		},
		{
			name: "vector positive offset",
			load: `load 30s
					http_requests_total{pod="nginx-1"} 1+1x15
					http_requests_total{pod="nginx-2"} 1+2x18`,
			query: `http_requests_total offset 30s`,
			start: time.Unix(600, 0),
			end:   time.Unix(1200, 0),
		},
		{
			name: "vector negative offset",
			load: `load 30s
					http_requests_total{pod="nginx-1"} 1+1x15
					http_requests_total{pod="nginx-2"} 1+2x18`,
			query: `http_requests_total offset -30s`,
			start: time.Unix(600, 0),
			end:   time.Unix(1200, 0),
		},
		{
			name: "matrix negative offset with sum_over_time",
			load: `load 30s
					http_requests_total{pod="nginx-1"} 1+1x25
					http_requests_total{pod="nginx-2"} 1+2x28`,
			query: `sum_over_time(http_requests_total[5m] offset 5m)`,
			start: time.Unix(600, 0),
			end:   time.Unix(6000, 0),
		},
		{
			name: "matrix negative offset with count_over_time",
			load: `load 30s
					http_requests_total{pod="nginx-1"} 1+1x15
					http_requests_total{pod="nginx-2"} 1+2x18`,
			query: `count_over_time(http_requests_total[5m] offset -2m)`,
			start: time.Unix(600, 0),
			end:   time.Unix(6000, 0),
		},
		{
			name: "@ vector time 10s",
			load: `load 30s
					http_requests_total{pod="nginx-1"} 1+1x15
					http_requests_total{pod="nginx-2"} 1+2x18`,
			query: "http_requests_total @ 10",
		},
		{
			name: "@ vector time 120s",
			load: `load 30s
					http_requests_total{pod="nginx-1"} 1+1x15
					http_requests_total{pod="nginx-2"} 1+2x18`,
			query: "http_requests_total @ 120",
		},
		{
			name: "@ vector time 360s",
			load: `load 30s
					http_requests_total{pod="nginx-1"} 1+1x15
					http_requests_total{pod="nginx-2"} 1+2x18`,
			query: "http_requests_total @ 360",
		},
		{
			name: "@ vector start",
			load: `load 30s
					http_requests_total{pod="nginx-1"} 1+1x15
					http_requests_total{pod="nginx-2"} 1+2x18`,
			query: "http_requests_total @ start()",
		},
		{
			name: "@ vector end",
			load: `load 30s
					http_requests_total{pod="nginx-1"} 1+1x15
					http_requests_total{pod="nginx-2"} 1+2x18`,
			query: "http_requests_total @ end()",
		},
		{
			name: "count_over_time @ start",
			load: `load 30s
					http_requests_total{pod="nginx-1"} 1+1x15
					http_requests_total{pod="nginx-2"} 1+2x18`,
			query: "count_over_time(http_requests_total[5m] @ start())",
		},
		{
			name: "sum_over_time @ end",
			load: `load 30s
					http_requests_total{pod="nginx-1"} 1+1x15
					http_requests_total{pod="nginx-2"} 1+2x18`,
			query: "sum_over_time(http_requests_total[5m] @ start())",
		},
		{
			name: "avg_over_time @ 180s",
			load: `load 30s
					http_requests_total{pod="nginx-1"} 1+1x15
					http_requests_total{pod="nginx-2"} 1+2x18`,
			query: "avg_over_time(http_requests_total[4m] @ 180)",
		},
		{
			name: "@ vector 240s offset 2m",
			load: `load 30s
					http_requests_total{pod="nginx-1"} 1+1x15
					http_requests_total{pod="nginx-2"} 1+2x18`,
			query: "http_requests_total @ 240 offset 2m",
		},
		{
			name: "avg_over_time @ 120s offset -2m",
			load: `load 30s
					http_requests_total{pod="nginx-1"} 1+1x15
					http_requests_total{pod="nginx-2"} 1+2x18`,
			query: "http_requests_total @ 120 offset -2m",
		},
		{
			name: "sum_over_time @ 180s offset 2m",
			load: `load 30s
					http_requests_total{pod="nginx-1"} 1+1x15
					http_requests_total{pod="nginx-2"} 1+2x18`,
			query: "sum_over_time(http_requests_total[5m] @ 180 offset 2m)",
		},
		{
			name: "selector merge",
			load: `load 30s
					http_requests_total{pod="nginx-1", ns="nginx"} 1+1x15
					http_requests_total{pod="nginx-2", ns="nginx"} 1+2x18
					http_requests_total{pod="nginx-3", ns="nginx"} 1+2x21`,
			query: `http_requests_total{pod=~"nginx-1", ns="nginx"} / on() group_left() sum(http_requests_total{ns="nginx"})`,
		},
		{
			name: "selector merge with different ranges",
			load: `load 30s
					http_requests_total{pod="nginx-1", ns="nginx"} 2+2x16
					http_requests_total{pod="nginx-2", ns="nginx"} 2+4x18
					http_requests_total{pod="nginx-3", ns="nginx"} 2+6x20`,
			query: `
	rate(http_requests_total{pod=~"nginx-1", ns="nginx"}[2m])
	+ on() group_left()
	sum(http_requests_total{ns="nginx"})`,
		},
		// Result is correct but this likely fails due to https://github.com/golang/go/issues/12025.
		// TODO(saswatamcode): Test NaN cases separately. https://github.com/thanos-community/promql-engine/issues/88
		// {
		// 	name: "scalar func with NaN",
		// 	load: `load 30s
		//  	http_requests_total{pod="nginx-1"} 1+1x15
		//  	http_requests_total{pod="nginx-2"} 1+2x18`,
		// 	query: `scalar(http_requests_total)`,
		// },
		{
			name: "scalar func with aggr",
			load: `load 30s
			http_requests_total{pod="nginx-1"} 1+1x15
			http_requests_total{pod="nginx-2"} 1+2x18`,
			query: `scalar(max(http_requests_total))`,
		},
		{
			name: "scalar func with aggr and number on right",
			load: `load 30s
			http_requests_total{pod="nginx-1"} 1+1x15
			http_requests_total{pod="nginx-2"} 1+2x18`,
			query: `scalar(max(http_requests_total)) + 10`,
		},
		{
			name: "scalar func with aggr and number on left",
			load: `load 30s
			http_requests_total{pod="nginx-1"} 1+1x15
			http_requests_total{pod="nginx-2"} 1+2x18`,
			query: `10 + scalar(max(http_requests_total))`,
		},
		{
			name: "quantile",
			load: `load 30s
				       http_requests_total{pod="nginx-1", series="1"} 1+1.1x40
				       http_requests_total{pod="nginx-2", series="2"} 2+2.3x50
				       http_requests_total{pod="nginx-4", series="3"} 5+2.4x50
				       http_requests_total{pod="nginx-5", series="1"} 8.4+2.3x50
				       http_requests_total{pod="nginx-6", series="2"} 2.3+2.3x50	`,
			query: "quantile(scalar(sum(http_requests_total)), rate(http_requests_total[1m]))",
		},
		{
			name: "clamp",
			load: `load 30s
			http_requests_total{pod="nginx-1"} 1+1x15
			http_requests_total{pod="nginx-2"} 1+2x18`,
			query: `clamp(http_requests_total, 5, 10)`,
		},
		{
			name: "clamp_min",
			load: `load 30s
			http_requests_total{pod="nginx-1"} 1+1x15
			http_requests_total{pod="nginx-2"} 1+2x18`,
			query: `clamp_min(http_requests_total, 10)`,
		},
		{
			name: "complex func query",
			load: `load 30s
			http_requests_total{pod="nginx-1"} 1+1x15
			http_requests_total{pod="nginx-2"} 1+2x18`,
			query: `clamp(1 - http_requests_total, 10 - 5, 10)`,
		},
		{
			name: "func within func query",
			load: `load 30s
			http_requests_total{pod="nginx-1"} 1+1x15
			http_requests_total{pod="nginx-2"} 1+2x18`,
			query: `clamp(irate(http_requests_total[30s]), 10 - 5, 10)`,
		},
		{
			name: "aggr within func query",
			load: `load 30s
			http_requests_total{pod="nginx-1"} 1+1x15
			http_requests_total{pod="nginx-2"} 1+2x18`,
			query: `clamp(rate(http_requests_total[30s]), 10 - 5, 10)`,
		},
		{
			name: "func with scalar arg that selects storage",
			load: `load 30s
			http_requests_total{pod="nginx-1"} 1+1x15
			http_requests_total{pod="nginx-2"} 1+2x18`,
			query: `clamp_min(http_requests_total, scalar(max(http_requests_total)))`,
		},
		{
			name: "func with scalar arg that selects storage + number",
			load: `load 30s
			http_requests_total{pod="nginx-1"} 1+1x15
			http_requests_total{pod="nginx-2"} 1+2x18`,
			query: `clamp_min(http_requests_total, scalar(max(http_requests_total)) + 10)`,
		},
		{
			name: "histogram quantile",
			load: `load 30s
			http_requests_total{pod="nginx-1", le="1"} 1+3x10
			http_requests_total{pod="nginx-2", le="1"} 2+3x10
			http_requests_total{pod="nginx-1", le="2"} 1+2x10
			http_requests_total{pod="nginx-2", le="2"} 2+2x10
			http_requests_total{pod="nginx-2", le="5"} 3+2x10
			http_requests_total{pod="nginx-1", le="+Inf"} 1+1x10
			http_requests_total{pod="nginx-2", le="+Inf"} 4+1x10`,
			query: `histogram_quantile(0.9, http_requests_total)`,
		},
		{
			name: "histogram quantile with sum",
			load: `load 30s
			http_requests_total{pod="nginx-1", le="1"} 1+3x10
			http_requests_total{pod="nginx-2", le="1"} 2+3x10
			http_requests_total{pod="nginx-1", le="2"} 1+2x10
			http_requests_total{pod="nginx-2", le="2"} 2+2x10
			http_requests_total{pod="nginx-2", le="5"} 3+2x10
			http_requests_total{pod="nginx-1", le="+Inf"} 1+1x10
			http_requests_total{pod="nginx-2", le="+Inf"} 4+1x10`,
			query: `histogram_quantile(0.9, sum by (pod, le) (http_requests_total))`,
		},
		// TODO(fpetkovski): Uncomment once support for testing NaNs is added.
		//{
		//	name: "histogram quantile with scalar operator",
		//	load: `load 30s
		//	quantile{pod="nginx-1", le="1"} 1+1x2
		//	http_requests_total{pod="nginx-1", le="1"} 1+3x10
		//	http_requests_total{pod="nginx-2", le="1"} 2+3x10
		//	http_requests_total{pod="nginx-1", le="2"} 1+2x10
		//	http_requests_total{pod="nginx-2", le="2"} 2+2x10
		//	http_requests_total{pod="nginx-2", le="5"} 3+2x10
		//	http_requests_total{pod="nginx-1", le="+Inf"} 1+1x10
		//	http_requests_total{pod="nginx-2", le="+Inf"} 4+1x10`,
		//	query: `histogram_quantile(scalar(max(quantile)), http_requests_total)`,
		//},
		{
			name: "topk",
			load: `load 30s
				http_requests_total{pod="nginx-1", series="1"} 1+1.1x40
				http_requests_total{pod="nginx-2", series="1"} 2+2.3x50
				http_requests_total{pod="nginx-4", series="2"} 5+2.4x50
				http_requests_total{pod="nginx-5", series="2"} 8.4+2.3x50
				http_requests_total{pod="nginx-6", series="2"} 2.3+2.3x50`,
			query: "topk(2, http_requests_total)",
			start: time.Unix(0, 0),
			end:   time.Unix(3000, 0),
			step:  2 * time.Second,
		},
		{
			name: "topk by",
			load: `load 30s
				http_requests_total{pod="nginx-1", series="1"} 1+1.1x40
				http_requests_total{pod="nginx-2", series="1"} 2+2.3x50
				http_requests_total{pod="nginx-4", series="2"} 5+2.4x50
				http_requests_total{pod="nginx-5", series="2"} 8.4+2.3x50
				http_requests_total{pod="nginx-6", series="2"} 2.3+2.3x50`,
			query: "topk(2, http_requests_total) by (series)",
			start: time.Unix(0, 0),
			end:   time.Unix(3000, 0),
			step:  2 * time.Second,
		},
		{
			name: "topk with simple expression",
			load: `load 30s
				http_requests_total{pod="nginx-1", series="1"} 1+1.1x40
				http_requests_total{pod="nginx-2", series="1"} 2+2.3x50
				http_requests_total{pod="nginx-4", series="2"} 5+2.4x50
				http_requests_total{pod="nginx-5", series="2"} 8.4+2.3x50
				http_requests_total{pod="nginx-6", series="2"} 2.3+2.3x50`,
			query: "topk(2 - 1, http_requests_total) by (series)",
			start: time.Unix(0, 0),
			end:   time.Unix(3000, 0),
			step:  2 * time.Second,
		},
		{
			name: "topk with expression",
			load: `load 30s
				http_requests_total{pod="nginx-1", series="1"} 1+1.1x40
				http_requests_total{pod="nginx-2", series="1"} 2+2.3x50
				http_requests_total{pod="nginx-4", series="2"} 5+2.4x50
				http_requests_total{pod="nginx-5", series="2"} 8.4+2.3x50
				http_requests_total{pod="nginx-6", series="2"} 2.3+2.3x50`,
			query: "topk(scalar(min(http_requests_total)), http_requests_total) by (series)",
			start: time.Unix(0, 0),
			end:   time.Unix(500, 0),
			step:  2 * time.Second,
		},
		{
			name: "topk with expression as argument not returning any value",
			load: `load 30s
				http_requests_total{pod="nginx-1", series="1"} 1+1.1x40
				http_requests_total{pod="nginx-2", series="1"} 2+2.3x50
				http_requests_total{pod="nginx-4", series="2"} 5+2.4x50
				http_requests_total{pod="nginx-5", series="2"} 8.4+2.3x50
				http_requests_total{pod="nginx-6", series="2"} 2.3+2.3x50`,
			query: "topk(scalar(min(non_existent_metric)), http_requests_total) by (series)",
			start: time.Unix(0, 0),
			end:   time.Unix(500, 0),
			step:  2 * time.Second,
		},
		{
			name: "bottomK",
			load: `load 30s
				http_requests_total{pod="nginx-1", series="1"} 1+1.1x40
				http_requests_total{pod="nginx-2", series="1"} 2+2.3x50
				http_requests_total{pod="nginx-4", series="2"} 5+2.4x50
				http_requests_total{pod="nginx-5", series="2"} 8.4+2.3x50
				http_requests_total{pod="nginx-6", series="2"} 2.3+2.3x50`,
			query: "bottomk(2, http_requests_total)",
			start: time.Unix(0, 0),
			end:   time.Unix(3000, 0),
			step:  2 * time.Second,
		},
		{
			name: "bottomK by",
			load: `load 30s
				http_requests_total{pod="nginx-1", series="1"} 1+1.1x40
				http_requests_total{pod="nginx-2", series="1"} 2+2.3x50
				http_requests_total{pod="nginx-4", series="2"} 5+2.4x50
				http_requests_total{pod="nginx-5", series="2"} 8.4+2.3x50
				http_requests_total{pod="nginx-6", series="2"} 2.3+2.3x50`,
			query: "bottomk(2, http_requests_total) by (series)",
			start: time.Unix(0, 0),
			end:   time.Unix(3000, 0),
			step:  2 * time.Second,
		},
	}

	disableOptimizerOpts := []bool{true, false}
	lookbackDeltas := []time.Duration{30 * time.Second, time.Minute, 5 * time.Minute, 10 * time.Minute}
	for _, lookbackDelta := range lookbackDeltas {
		opts.LookbackDelta = lookbackDelta
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				test, err := promql.NewTest(t, tc.load)
				testutil.Ok(t, err)
				defer test.Close()

				testutil.Ok(t, test.Run())

				if tc.start.Equal(time.Time{}) {
					tc.start = start
				}
				if tc.end.Equal(time.Time{}) {
					tc.end = end
				}
				if tc.step == 0 {
					tc.step = step
				}
				for _, disableOptimizers := range disableOptimizerOpts {
					t.Run(fmt.Sprintf("disableOptimizers=%v", disableOptimizers), func(t *testing.T) {
						for _, disableFallback := range []bool{false, true} {
							t.Run(fmt.Sprintf("disableFallback=%v", disableFallback), func(t *testing.T) {
								newEngine := engine.New(engine.Opts{EngineOpts: opts, DisableFallback: disableFallback, DisableOptimizers: disableOptimizers})
								q1, err := newEngine.NewRangeQuery(test.Storage(), nil, tc.query, tc.start, tc.end, tc.step)
								testutil.Ok(t, err)
								defer q1.Close()

								newResult := q1.Exec(context.Background())

								oldEngine := promql.NewEngine(opts)
								q2, err := oldEngine.NewRangeQuery(test.Storage(), nil, tc.query, tc.start, tc.end, tc.step)
								testutil.Ok(t, err)
								defer q2.Close()

								oldResult := q2.Exec(context.Background())
								if oldResult.Err == nil {
									testutil.Ok(t, newResult.Err)
									testutil.Equals(t, oldResult, newResult)
								} else {
									testutil.NotOk(t, newResult.Err)
								}
							})
						}
					})
				}
			})
		}
	}
}

func TestBinopEdgeCases(t *testing.T) {
	opts := promql.EngineOpts{
		Timeout:              1 * time.Hour,
		MaxSamples:           1e10,
		EnableNegativeOffset: true,
		EnableAtModifier:     true,
	}

	series := []storage.Series{
		newMockSeries(
			[]string{labels.MetricName, "foo"},
			[]int64{0, 30000, 60000, 1200000, 1500000, 1800000},
			[]float64{1, 2, 3, 4, 5, 6},
		),
		newMockSeries(
			[]string{labels.MetricName, "bar", "id", "1"},
			[]int64{0, 30000},
			[]float64{1, 2},
		),
		newMockSeries(
			[]string{labels.MetricName, "bar", "id", "2"},
			[]int64{1200000, 1500000},
			[]float64{3, 4},
		),
	}
	query := `foo * on () group_left bar`

	start := time.Unix(0, 0)
	end := time.Unix(30000, 0)
	step := time.Second * 30

	oldEngine := promql.NewEngine(opts)
	q1, err := oldEngine.NewRangeQuery(storageWithSeries(series...), nil, query, start, end, step)
	testutil.Ok(t, err)

	newEngine := engine.New(engine.Opts{})
	q2, err := newEngine.NewRangeQuery(storageWithSeries(series...), nil, query, start, end, step)
	testutil.Ok(t, err)

	ctx := context.Background()
	oldResult := q1.Exec(ctx)

	newResult := q2.Exec(ctx)
	testutil.Equals(t, oldResult, newResult)
}

func TestInstantQuery(t *testing.T) {
	defaultQueryTime := time.Unix(50, 0)
	// Negative offset and at modifier are enabled by default
	// since Prometheus v2.33.0, so we also enable them.
	opts := promql.EngineOpts{
		Timeout:              1 * time.Hour,
		MaxSamples:           1e10,
		EnableNegativeOffset: true,
		EnableAtModifier:     true,
	}

	cases := []struct {
		load         string
		name         string
		query        string
		queryTime    time.Time
		sortByLabels bool // if true, the series in the result between the old and new engine should be sorted before compared
	}{
		{
			name:      "scalar",
			load:      ``,
			queryTime: time.Unix(160, 0),
			query:     "12 + 1",
		},
		{
			name: "increase plus offset",
			load: `load 1s
			http_requests_total{pod="nginx-1"} 1+1x180`,
			queryTime: time.Unix(160, 0),
			query:     "increase(http_requests_total[1m] offset 1m)",
		},
		{
			name: "quantile by pod",
			load: `load 30s
				       http_requests_total{pod="nginx-1", series="1"} 1+1.1x40
				       http_requests_total{pod="nginx-2", series="2"} 2+2.3x50
				       http_requests_total{pod="nginx-4", series="3"} 5+2.4x50
				       http_requests_total{pod="nginx-5", series="1"} 8.4+2.3x50
				       http_requests_total{pod="nginx-6", series="2"} 2.3+2.3x50`,
			query: "quantile by (pod) (0.9, rate(http_requests_total[1m]))",
		},
		{
			name: "quantile by pod with binary",
			load: `load 30s
				       http_requests_total{pod="nginx-1", series="1"} 1+1.1x40
				       http_requests_total{pod="nginx-2", series="2"} 2+2.3x50
				       http_requests_total{pod="nginx-4", series="3"} 5+2.4x50
				       http_requests_total{pod="nginx-5", series="1"} 8.4+2.3x50
				       http_requests_total{pod="nginx-6", series="2"} 2.3+2.3x50`,
			query: "quantile by (pod) (1 - 0.1, rate(http_requests_total[1m]))",
		},
		{
			name: "quantile by pod with expression",
			load: `load 30s
				       http_requests_total{pod="nginx-1", series="1"} 1+1.1x40
				       http_requests_total{pod="nginx-2", series="2"} 2+2.3x50
				       http_requests_total{pod="nginx-4", series="3"} 5+2.4x50
				       http_requests_total{pod="nginx-5", series="1"} 8.4+2.3x50
				       http_requests_total{pod="nginx-6", series="2"} 2.3+2.3x50`,
			query: "quantile by (pod) (scalar(min(http_requests_total)), rate(http_requests_total[1m]))",
		},
		{
			name: "quantile",
			load: `load 30s
				       http_requests_total{pod="nginx-1", series="1"} 1+1.1x40
				       http_requests_total{pod="nginx-2", series="2"} 2+2.3x50
				       http_requests_total{pod="nginx-4", series="3"} 5+2.4x50
				       http_requests_total{pod="nginx-5", series="1"} 8.4+2.3x50
				       http_requests_total{pod="nginx-6", series="2"} 2.3+2.3x50	`,
			query: "quantile(0.9, rate(http_requests_total[1m]))",
		},
		{
			name: "stdvar",
			load: `load 30s
						http_requests_total{pod="nginx-1"} 1+1x4
						http_requests_total{pod="nginx-2"} 1+2x4`,
			query: "stdvar(http_requests_total)",
		},
		{
			name: "stdvar by pod",
			load: `load 30s
						http_requests_total{pod="nginx-1"} 1
						http_requests_total{pod="nginx-2"} 2
						http_requests_total{pod="nginx-3"} 8
						http_requests_total{pod="nginx-4"} 6`,
			query: "stdvar by (pod) (http_requests_total)",
		},
		{
			name: "stddev",
			load: `load 30s
						http_requests_total{pod="nginx-1"} 1+1x4
						http_requests_total{pod="nginx-2"} 1+2x4`,
			query: "stddev(http_requests_total)",
		},
		{
			name: "stddev by pod",
			load: `load 30s
						http_requests_total{pod="nginx-1"} 1
						http_requests_total{pod="nginx-2"} 2
						http_requests_total{pod="nginx-3"} 8
						http_requests_total{pod="nginx-4"} 6`,
			query: "stddev by (pod) (http_requests_total)",
		},
		{
			name: "sum by pod",
			load: `load 30s
						http_requests_total{pod="nginx-1"} 1+1x4
						http_requests_total{pod="nginx-2"} 1+2x4`,
			query: "sum by (pod) (http_requests_total)",
		},
		{
			name: "count",
			load: `load 30s
						http_requests_total{pod="nginx-1"} 1+1x15
						http_requests_total{pod="nginx-2"} 1+2x18`,
			query: "count(http_requests_total)",
		},
		{
			name: "average",
			load: `load 30s
						http_requests_total{pod="nginx-1"} 1+1x15
						http_requests_total{pod="nginx-2"} 1+2x18`,
			query: "avg(http_requests_total)",
		},
		{
			name: "topk",
			load: `load 30s
						http_requests_total{pod="nginx-1", series="1"} 1
						http_requests_total{pod="nginx-2", series="1"} 2
						http_requests_total{pod="nginx-3", series="1"} 8
						http_requests_total{pod="nginx-4", series="2"} 6
						http_requests_total{pod="nginx-5", series="2"} 8
						http_requests_total{pod="nginx-6", series="3"} 15
						http_requests_total{pod="nginx-7", series="3"} 11
						http_requests_total{pod="nginx-8", series="4"} 22
						http_requests_total{pod="nginx-9", series="4"} 89`,
			query: "topk(2, http_requests_total)",
			// TODO (alanprot): Top/BottomK series order on result. https://github.com/thanos-community/promql-engine/issues/120
			sortByLabels: true,
		},
		{
			name: "topk by series",
			load: `load 30s
						http_requests_total{pod="nginx-1", series="1"} 1
						http_requests_total{pod="nginx-2", series="1"} 2
						http_requests_total{pod="nginx-3", series="1"} 8
						http_requests_total{pod="nginx-4", series="2"} 6
						http_requests_total{pod="nginx-5", series="2"} 8
						http_requests_total{pod="nginx-6", series="3"} 15
						http_requests_total{pod="nginx-7", series="3"} 11
						http_requests_total{pod="nginx-8", series="4"} 22
						http_requests_total{pod="nginx-9", series="4"} 89`,
			query:        "topk(2, http_requests_total) by (series)",
			sortByLabels: true,
		},
		{
			name: "bottomK",
			load: `load 30s
						http_requests_total{pod="nginx-1", series="1"} 1
						http_requests_total{pod="nginx-2", series="1"} 2
						http_requests_total{pod="nginx-3", series="1"} 8
						http_requests_total{pod="nginx-4", series="2"} 6
						http_requests_total{pod="nginx-5", series="2"} 8
						http_requests_total{pod="nginx-6", series="3"} 15
						http_requests_total{pod="nginx-7", series="3"} 11
						http_requests_total{pod="nginx-8", series="4"} 22
						http_requests_total{pod="nginx-9", series="4"} 89`,
			query:        "bottomk(2, http_requests_total)",
			sortByLabels: true,
		},
		{
			name: "bottomk by series",
			load: `load 30s
						http_requests_total{pod="nginx-1", series="1"} 1
						http_requests_total{pod="nginx-2", series="1"} 2
						http_requests_total{pod="nginx-3", series="1"} 8
						http_requests_total{pod="nginx-4", series="2"} 6
						http_requests_total{pod="nginx-5", series="2"} 8
						http_requests_total{pod="nginx-6", series="3"} 15
						http_requests_total{pod="nginx-7", series="3"} 11
						http_requests_total{pod="nginx-8", series="4"} 22
						http_requests_total{pod="nginx-9", series="4"} 89`,
			query:        "bottomk(2, http_requests_total) by (series)",
			sortByLabels: true,
		},
		{
			name: "max",
			load: `load 30s
						http_requests_total{pod="nginx-1"} 1+1x15
						http_requests_total{pod="nginx-2"} 1+2x18`,
			query: "max(http_requests_total)",
		},
		{
			name: "max with only 1 sample",
			load: `load 30s
						http_requests_total{pod="nginx-1"} -1
						http_requests_total{pod="nginx-2"} 1`,
			query: "max(http_requests_total) by (pod)",
		},
		{
			name: "min",
			load: `load 30s
						http_requests_total{pod="nginx-1"} 1+1x15
						http_requests_total{pod="nginx-2"} 1+2x18`,
			query: "min(http_requests_total)",
		},
		{
			name: "min with only 1 sample",
			load: `load 30s
						http_requests_total{pod="nginx-1"} -1
						http_requests_total{pod="nginx-2"} 1`,
			query: "min(http_requests_total) by (pod)",
		},
		{
			name: "rate",
			load: `load 30s
					http_requests_total{pod="nginx-1", series="1"} 1+1.1x40
					http_requests_total{pod="nginx-2", series="2"} 2+2.3x50
					http_requests_total{pod="nginx-4", series="3"} 5+2.4x50
					http_requests_total{pod="nginx-5", series="1"} 8.4+2.3x50
					http_requests_total{pod="nginx-6", series="2"} 2.3+2.3x50`,
			query: "rate(http_requests_total[1m])",
		},
		{
			name: "sum rate",
			load: `load 30s
						http_requests_total{pod="nginx-1"} 1+1x4
						http_requests_total{pod="nginx-2"} 1+2x4`,
			query: "sum(rate(http_requests_total[1m]))",
		},
		{
			name: "sum rate with single sample series",
			load: `load 30s
						http_requests_total{pod="nginx-1"} 1+1x4
						http_requests_total{pod="nginx-2"} 1+2x4
						http_requests_total{pod="nginx-3"} 0`,
			query: "sum by (pod) (rate(http_requests_total[1m]))",
		},
		{
			name: "sum rate with stale series",
			load: `load 30s
						http_requests_total{pod="nginx-1"} 1+1x4
						http_requests_total{pod="nginx-2"} 1+2x20`,
			query: "sum(rate(http_requests_total[1m]))",
		},
		{
			name: "delta",
			load: `load 30s
						http_requests_total{pod="nginx-1"} 1+1x4
						http_requests_total{pod="nginx-2"} 1+2x4`,
			query: "delta(http_requests_total[1m])",
		},
		{
			name: "increase",
			load: `load 30s
						http_requests_total{pod="nginx-1"} 1+1x4
						http_requests_total{pod="nginx-2"} 1+2x4`,
			query: "increase(http_requests_total[1m])",
		},

		{
			name: "sum irate",
			load: `load 30s
						http_requests_total{pod="nginx-1"} 1+1x4
						http_requests_total{pod="nginx-2"} 1+2x4`,
			query: "sum(irate(http_requests_total[1m]))",
		},
		{
			name: "sum irate with stale series",
			load: `load 30s
						http_requests_total{pod="nginx-1"} 1+1x4
						http_requests_total{pod="nginx-2"} 1+2x20`,
			query: "sum(irate(http_requests_total[1m]))",
		},
		{
			name:  "number literal",
			load:  "",
			query: "34",
		},
		{
			name:  "vector",
			load:  "",
			query: "vector(24)",
		},
		{
			name: "binary operation with vector and scalar on the right",
			load: `load 30s
					foo{method="get", code="500"} 1+1.1x30
					foo{method="get", code="404"} 1+2.2x20`,
			query: `foo * 2`,
		},
		{
			name: "binary operation with vector and scalar on the left",
			load: `load 30s
					foo{method="get", code="500"} 1+1.1x30
					foo{method="get", code="404"} 1+2.2x20`,
			query: `2 * foo`,
		},
		{
			name: "complex binary operation",
			load: `load 30s
					foo{method="get", code="500"} 1+1.1x30
					foo{method="get", code="404"} 1+2.2x20`,
			query: `1 - (100 * sum(foo{method="get"}) / sum(foo))`,
		},
		{
			name: "vector binary op ==",
			load: `load 30s
					foo{method="get", code="500"} 1+1x40
					bar{method="get", code="404"} 1+1.1x30`,
			query: `sum(foo) by (method) == sum(bar) by (method)`,
		},
		{
			name: "vector binary op !=",
			load: `load 30s
					foo{method="get", code="500"} 1+1x40
					bar{method="get", code="404"} 1+1.1x30`,
			query: `sum(foo) by (method) != sum(bar) by (method)`,
		},
		{
			name: "vector binary op >",
			load: `load 30s
					foo{method="get", code="500"} 1+1x40
					bar{method="get", code="404"} 1+1.1x30`,
			query: `sum(foo) by (method) > sum(bar) by (method)`,
		},
		{
			name: "vector binary op <",
			load: `load 30s
					foo{method="get", code="500"} 1+1x40
					bar{method="get", code="404"} 1+1.1x30`,
			query: `sum(foo) by (method) < sum(bar) by (method)`,
		},
		{
			name: "vector binary op >=",
			load: `load 30s
					foo{method="get", code="500"} 1+1x40
					bar{method="get", code="404"} 1+1.1x30`,
			query: `sum(foo) by (method) >= sum(bar) by (method)`,
		},
		{
			name: "vector binary op <=",
			load: `load 30s
					foo{method="get", code="500"} 1+1x40
					bar{method="get", code="404"} 1+1.1x30`,
			query: `sum(foo) by (method) <= sum(bar) by (method)`,
		},
		{
			name: "vector binary op ^",
			load: `load 30s
					foo{method="get", code="500"} 1+1x40
					bar{method="get", code="404"} 1+1.1x30`,
			query: `sum(foo) by (method) ^ sum(bar) by (method)`,
		},
		{
			name: "vector binary op %",
			load: `load 30s
					foo{method="get", code="500"} 1+2x40
					bar{method="get", code="404"} 1+1x30`,
			query: `sum(foo) by (method) % sum(bar) by (method)`,
		},
		{
			name: "vector binary op > scalar",
			load: `load 30s
					foo{method="get", code="500"} 1+2x40
					bar{method="get", code="404"} 1+1x30`,
			query: `sum(foo) by (method) > 10`,
		},
		{
			name: "scalar < vector binary op",
			load: `load 30s
					foo{method="get", code="500"} 1+2x40
					bar{method="get", code="404"} 1+1x30`,
			query: `10 < sum(foo) by (method)`,
		},
		{
			name:  "scalar binary op == true",
			load:  ``,
			query: `1 == bool 1`,
		},
		{
			name:  "scalar binary op == false",
			load:  ``,
			query: `1 != bool 2`,
		},
		{
			name:  "scalar binary op !=",
			load:  ``,
			query: `1 != bool 1`,
		},
		{
			name:  "scalar binary op >",
			load:  ``,
			query: `1 > bool 0`,
		},
		{
			name:  "scalar binary op <",
			load:  ``,
			query: `1 > bool 2`,
		},
		{
			name:  "scalar binary op >=",
			load:  ``,
			query: `1 >= bool 0`,
		},
		{
			name:  "scalar binary op <=",
			load:  ``,
			query: `1 <= bool 2`,
		},
		{
			name:  "scalar binary op % 0",
			load:  ``,
			query: `2 % 2`,
		},
		{
			name:  "scalar binary op % 1",
			load:  ``,
			query: `1 % 2`,
		},
		{
			name:  "scalar binary op ^",
			load:  ``,
			query: `2 ^ 2`,
		},
		{
			name:  "empty series",
			load:  "",
			query: "http_requests_total",
		},
		{
			name:  "empty series with func",
			load:  "",
			query: "sum(http_requests_total)",
		},
		{
			name: "empty result",
			load: `load 30s
						http_requests_total{pod="nginx-1"} 1+1x15
						http_requests_total{pod="nginx-2"} 1+2x18`,
			query: `http_requests_total{pod="nginx-3"}`,
		},
		{
			name: "last_over_time",
			load: `load 30s
						http_requests_total{pod="nginx-1"} 1+1x15
						http_requests_total{pod="nginx-2"} 1+2x18`,
			query: `last_over_time(http_requests_total[30s])`,
		},
		{
			name: "group",
			load: `load 30s
						http_requests_total{pod="nginx-1"} 1+1x15
						http_requests_total{pod="nginx-2"} 1+2x18`,
			query: `group(http_requests_total)`,
		},
		{
			name: "reset",
			load: `load 30s
						http_requests_total{pod="nginx-1"} 100-1x15
						http_requests_total{pod="nginx-2"} 1+2x18`,
			query: `resets(http_requests_total[5m])`,
		},
		{
			name: "present_over_time",
			load: `load 30s
						http_requests_total{pod="nginx-1"} 1+1x15
						http_requests_total{pod="nginx-2"} 1+2x18`,
			query: `present_over_time(http_requests_total[30s])`,
		},
		{
			name:  "unary sub operation for scalar",
			load:  ``,
			query: `-(1 + 5)`,
		},
		{
			name: "unary sub operation for vector",
			load: `load 30s
						http_requests_total{pod="nginx-1"} 1+1x15
						http_requests_total{pod="nginx-2"} 1+2x18`,
			query: `-http_requests_total`,
		},
		{
			name: "unary add operation for vector",
			load: `load 30s
						http_requests_total{pod="nginx-1"} 1+1x15
						http_requests_total{pod="nginx-2"} 1+2x18`,
			query: `+http_requests_total`,
		},
		{
			name: "vector positive offset",
			load: `load 30s
						http_requests_total{pod="nginx-1"} 1+1x15
						http_requests_total{pod="nginx-2"} 1+2x18`,
			query: `http_requests_total offset 30s`,
		},
		{
			name: "vector negative offset",
			load: `load 30s
						http_requests_total{pod="nginx-1"} 1+1x15
						http_requests_total{pod="nginx-2"} 1+2x18`,
			query: `http_requests_total offset -30s`,
		},
		{
			name: "matrix negative offset with sum_over_time",
			load: `load 30s
						http_requests_total{pod="nginx-1"} 1+1x25
						http_requests_total{pod="nginx-2"} 1+2x28`,
			query: `sum_over_time(http_requests_total[5m] offset 5m)`,
		},
		{
			name: "matrix negative offset with count_over_time",
			load: `load 30s
						http_requests_total{pod="nginx-1"} 1+1x15
						http_requests_total{pod="nginx-2"} 1+2x18`,
			query: `count_over_time(http_requests_total[5m] offset -2m)`,
		},
		{
			name: "@ vector time 10s",
			load: `load 30s
						http_requests_total{pod="nginx-1"} 1+1x15
						http_requests_total{pod="nginx-2"} 1+2x18`,
			query: "http_requests_total @ 10",
		},
		{
			name: "@ vector time 120s",
			load: `load 30s
						http_requests_total{pod="nginx-1"} 1+1x15
						http_requests_total{pod="nginx-2"} 1+2x18`,
			query: "http_requests_total @ 120",
		},
		{
			name: "@ vector time 360s",
			load: `load 30s
						http_requests_total{pod="nginx-1"} 1+1x15
						http_requests_total{pod="nginx-2"} 1+2x18`,
			query: "http_requests_total @ 360",
		},
		{
			name: "@ vector start",
			load: `load 30s
						http_requests_total{pod="nginx-1"} 1+1x15
						http_requests_total{pod="nginx-2"} 1+2x18`,
			query: "http_requests_total @ start()",
		},
		{
			name: "@ vector end",
			load: `load 30s
						http_requests_total{pod="nginx-1"} 1+1x15
						http_requests_total{pod="nginx-2"} 1+2x18`,
			query: "http_requests_total @ end()",
		},
		{
			name: "count_over_time @ start",
			load: `load 30s
						http_requests_total{pod="nginx-1"} 1+1x15
						http_requests_total{pod="nginx-2"} 1+2x18`,
			query: "count_over_time(http_requests_total[5m] @ start())",
		},
		{
			name: "sum_over_time @ end",
			load: `load 30s
						http_requests_total{pod="nginx-1"} 1+1x15
						http_requests_total{pod="nginx-2"} 1+2x18`,
			query: "sum_over_time(http_requests_total[5m] @ start())",
		},
		{
			name: "avg_over_time @ 180s",
			load: `load 30s
						http_requests_total{pod="nginx-1"} 1+1x15
						http_requests_total{pod="nginx-2"} 1+2x18`,
			query: "avg_over_time(http_requests_total[4m] @ 180)",
		},
		{
			name: "@ vector 240s offset 2m",
			load: `load 30s
						http_requests_total{pod="nginx-1"} 1+1x15
						http_requests_total{pod="nginx-2"} 1+2x18`,
			query: "http_requests_total @ 240 offset 2m",
		},
		{
			name: "avg_over_time @ 120s offset -2m",
			load: `load 30s
						http_requests_total{pod="nginx-1"} 1+1x15
						http_requests_total{pod="nginx-2"} 1+2x18`,
			query: "http_requests_total @ 120 offset -2m",
		},
		{
			name: "sum_over_time @ 180s offset 2m",
			load: `load 30s
						http_requests_total{pod="nginx-1"} 1+1x15
						http_requests_total{pod="nginx-2"} 1+2x18`,
			query: "sum_over_time(http_requests_total[5m] @ 180 offset 2m)",
		},
		// Result is correct but this likely fails due to https://github.com/golang/go/issues/12025.
		// TODO(saswatamcode): Test NaN cases separately. https://github.com/thanos-community/promql-engine/issues/88
		// {
		// 	name: "scalar func with NaN",
		// 	load: `load 30s
		//  	http_requests_total{pod="nginx-1"} 1+1x15
		//  	http_requests_total{pod="nginx-2"} 1+2x18`,
		// 	query: `scalar(http_requests_total)`,
		// },
		{
			name: "scalar func with aggr",
			load: `load 30s
			http_requests_total{pod="nginx-1"} 1+1x15
			http_requests_total{pod="nginx-2"} 1+2x18`,
			query: `scalar(max(http_requests_total))`,
		},
		{
			name: "scalar func with aggr and number on right",
			load: `load 30s
			http_requests_total{pod="nginx-1"} 1+1x15
			http_requests_total{pod="nginx-2"} 1+2x18`,
			query: `scalar(max(http_requests_total)) + 10`,
		},
		{
			name: "scalar func with aggr and number on left",
			load: `load 30s
			http_requests_total{pod="nginx-1"} 1+1x15
			http_requests_total{pod="nginx-2"} 1+2x18`,
			query: `10 + scalar(max(http_requests_total))`,
		},
		{
			name: "clamp",
			load: `load 30s
				http_requests_total{pod="nginx-1"} 1+1x15
				http_requests_total{pod="nginx-2"} 1+2x18`,
			query: `clamp(http_requests_total, 5, 10)`,
		},
		{
			name: "clamp_min",
			load: `load 30s
				http_requests_total{pod="nginx-1"} 1+1x15
				http_requests_total{pod="nginx-2"} 1+2x18`,
			query: `clamp_min(http_requests_total, 10)`,
		},
		{
			name: "complex func query",
			load: `load 30s
				http_requests_total{pod="nginx-1"} 1+1x15
				http_requests_total{pod="nginx-2"} 1+2x18`,
			query: `clamp(1 - http_requests_total, 10 - 5, 10)`,
		},
		{
			name: "func within func query",
			load: `load 30s
				http_requests_total{pod="nginx-1"} 1+1x15
				http_requests_total{pod="nginx-2"} 1+2x18`,
			query: `clamp(irate(http_requests_total[30s]), 10 - 5, 10)`,
		},
		{
			name: "aggr within func query",
			load: `load 30s
				http_requests_total{pod="nginx-1"} 1+1x15
				http_requests_total{pod="nginx-2"} 1+2x18`,
			query: `clamp(rate(http_requests_total[30s]), 10 - 5, 10)`,
		},
		{
			name: "func with scalar arg that selects storage",
			load: `load 30s
				http_requests_total{pod="nginx-1"} 1+1x15
				http_requests_total{pod="nginx-2"} 1+2x18`,
			query: `clamp_min(http_requests_total, scalar(max(http_requests_total)))`,
		},
		{
			name: "func with scalar arg that selects storage + number",
			load: `load 30s
				http_requests_total{pod="nginx-1"} 1+1x15
				http_requests_total{pod="nginx-2"} 1+2x18`,
			query: `clamp_min(http_requests_total, scalar(max(http_requests_total)) + 10)`,
		},
	}

	disableOptimizers := []bool{true, false}
	lookbackDeltas := []time.Duration{30 * time.Second, time.Minute, 5 * time.Minute, 10 * time.Minute}
	for _, withoutOptimizers := range disableOptimizers {
		t.Run(fmt.Sprintf("disableOptimizers=%t", withoutOptimizers), func(t *testing.T) {
			for _, lookbackDelta := range lookbackDeltas {
				opts.LookbackDelta = lookbackDelta
				for _, tc := range cases {
					t.Run(tc.name, func(t *testing.T) {
						test, err := promql.NewTest(t, tc.load)
						testutil.Ok(t, err)
						defer test.Close()

						testutil.Ok(t, test.Run())

						for _, disableFallback := range []bool{false, true} {
							t.Run(fmt.Sprintf("disableFallback=%v", disableFallback), func(t *testing.T) {
								var queryTime time.Time = defaultQueryTime
								if tc.queryTime != (time.Time{}) {
									queryTime = tc.queryTime
								}

								newEngine := engine.New(engine.Opts{EngineOpts: opts, DisableFallback: disableFallback})
								q1, err := newEngine.NewInstantQuery(test.Storage(), nil, tc.query, queryTime)
								testutil.Ok(t, err)
								defer q1.Close()

								newResult := q1.Exec(context.Background())
								testutil.Ok(t, newResult.Err)

								oldEngine := promql.NewEngine(opts)
								q2, err := oldEngine.NewInstantQuery(test.Storage(), nil, tc.query, queryTime)
								testutil.Ok(t, err)
								defer q2.Close()

								oldResult := q2.Exec(context.Background())
								testutil.Ok(t, oldResult.Err)

								if tc.sortByLabels {
									sortByLabels(oldResult)
									sortByLabels(newResult)
								}

								testutil.Equals(t, oldResult, newResult)
							})
						}
					})
				}
			}
		})
	}
}

func TestQueryCancellation(t *testing.T) {
	twelveHours := int64(12 * time.Hour.Seconds())

	start := time.Unix(0, 0)
	end := time.Unix(twelveHours, 0)
	step := time.Second * 30
	query := `sum(rate(http_requests_total{pod="nginx-1"}[10s]))`
	load := `load 30s
				http_requests_total{pod="nginx-1"} 1+1x1
				http_requests_total{pod="nginx-2"} 1+2x40`

	test, err := promql.NewTest(t, load)
	testutil.Ok(t, err)
	defer test.Close()

	testutil.Ok(t, test.Run())

	querier := &storage.MockQueryable{
		MockQuerier: &storage.MockQuerier{
			SelectMockFunction: func(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
				return newTestSeriesSet(&slowSeries{})
			},
		},
	}

	newEngine := engine.New(engine.Opts{})
	q1, err := newEngine.NewRangeQuery(querier, nil, query, start, end, step)
	testutil.Ok(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-time.After(1000 * time.Millisecond)
		cancel()
	}()

	newResult := q1.Exec(ctx)
	testutil.Equals(t, context.Canceled, newResult.Err)
}

type hintRecordingQuerier struct {
	storage.Querier
	hints []*storage.SelectHints
}

func (h *hintRecordingQuerier) Close() error { return nil }

func (h *hintRecordingQuerier) Select(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	h.hints = append(h.hints, hints)
	return storage.EmptySeriesSet()
}

func TestSelectHintsSetCorrectly(t *testing.T) {
	for _, tc := range []struct {
		query string

		// All times are in milliseconds.
		start int64
		end   int64

		// TODO(bwplotka): Add support for better hints when subquerying.
		expected []*storage.SelectHints
	}{
		{
			query: "foo", start: 10000,
			expected: []*storage.SelectHints{
				{Start: 5000, End: 10000},
			},
		}, {
			query: "foo @ 15", start: 10000,
			expected: []*storage.SelectHints{
				{Start: 10000, End: 15000},
			},
		}, {
			query: "foo @ 1", start: 10000,
			expected: []*storage.SelectHints{
				{Start: -4000, End: 1000},
			},
		}, {
			query: "foo[2m]", start: 200000,
			expected: []*storage.SelectHints{
				{Start: 80000, End: 200000, Range: 120000},
			},
		}, {
			query: "foo[2m] @ 180", start: 200000,
			expected: []*storage.SelectHints{
				{Start: 60000, End: 180000, Range: 120000},
			},
		}, {
			query: "foo[2m] @ 300", start: 200000,
			expected: []*storage.SelectHints{
				{Start: 180000, End: 300000, Range: 120000},
			},
		}, {
			query: "foo[2m] @ 60", start: 200000,
			expected: []*storage.SelectHints{
				{Start: -60000, End: 60000, Range: 120000},
			},
		}, {
			query: "foo[2m] offset 2m", start: 300000,
			expected: []*storage.SelectHints{
				{Start: 60000, End: 180000, Range: 120000},
			},
		}, {
			query: "foo[2m] @ 200 offset 2m", start: 300000,
			expected: []*storage.SelectHints{
				{Start: -40000, End: 80000, Range: 120000},
			},
		}, {
			query: "foo[2m:1s]", start: 300000,
			expected: []*storage.SelectHints{
				{Start: 175000, End: 300000, Step: 1000},
			},
		}, {
			query: "count_over_time(foo[2m:1s])", start: 300000,
			expected: []*storage.SelectHints{
				{Start: 175000, End: 300000, Func: "count_over_time", Step: 1000},
			},
		}, {
			query: "count_over_time(foo[2m:1s] @ 300)", start: 200000,
			expected: []*storage.SelectHints{
				{Start: 175000, End: 300000, Func: "count_over_time", Step: 1000},
			},
		}, {
			query: "count_over_time(foo[2m:1s] @ 200)", start: 200000,
			expected: []*storage.SelectHints{
				{Start: 75000, End: 200000, Func: "count_over_time", Step: 1000},
			},
		}, {
			query: "count_over_time(foo[2m:1s] @ 100)", start: 200000,
			expected: []*storage.SelectHints{
				{Start: -25000, End: 100000, Func: "count_over_time", Step: 1000},
			},
		}, {
			query: "count_over_time(foo[2m:1s] offset 10s)", start: 300000,
			expected: []*storage.SelectHints{
				{Start: 165000, End: 290000, Func: "count_over_time", Step: 1000},
			},
		}, {
			query: "count_over_time((foo offset 10s)[2m:1s] offset 10s)", start: 300000,
			expected: []*storage.SelectHints{
				{Start: 155000, End: 280000, Func: "count_over_time", Step: 1000},
			},
		}, {
			// When the @ is on the vector selector, the enclosing subquery parameters
			// don't affect the hint ranges.
			query: "count_over_time((foo @ 200 offset 10s)[2m:1s] offset 10s)", start: 300000,
			expected: []*storage.SelectHints{
				{Start: 185000, End: 190000, Func: "count_over_time", Step: 1000},
			},
		}, {
			// When the @ is on the vector selector, the enclosing subquery parameters
			// don't affect the hint ranges.
			query: "count_over_time((foo @ 200 offset 10s)[2m:1s] @ 100 offset 10s)", start: 300000,
			expected: []*storage.SelectHints{
				{Start: 185000, End: 190000, Func: "count_over_time", Step: 1000},
			},
		}, {
			query: "count_over_time((foo offset 10s)[2m:1s] @ 100 offset 10s)", start: 300000,
			expected: []*storage.SelectHints{
				{Start: -45000, End: 80000, Func: "count_over_time", Step: 1000},
			},
		}, {
			query: "foo", start: 10000, end: 20000,
			expected: []*storage.SelectHints{
				{Start: 5000, End: 20000, Step: 1000},
			},
		}, {
			query: "foo @ 15", start: 10000, end: 20000,
			expected: []*storage.SelectHints{
				{Start: 10000, End: 15000, Step: 1000},
			},
		}, {
			query: "foo @ 1", start: 10000, end: 20000,
			expected: []*storage.SelectHints{
				{Start: -4000, End: 1000, Step: 1000},
			},
		}, {
			query: "rate(foo[2m] @ 180)", start: 200000, end: 500000,
			expected: []*storage.SelectHints{
				{Start: 60000, End: 180000, Range: 120000, Func: "rate", Step: 1000},
			},
		}, {
			query: "rate(foo[2m] @ 300)", start: 200000, end: 500000,
			expected: []*storage.SelectHints{
				{Start: 180000, End: 300000, Range: 120000, Func: "rate", Step: 1000},
			},
		}, {
			query: "rate(foo[2m] @ 60)", start: 200000, end: 500000,
			expected: []*storage.SelectHints{
				{Start: -60000, End: 60000, Range: 120000, Func: "rate", Step: 1000},
			},
		}, {
			query: "rate(foo[2m])", start: 200000, end: 500000,
			expected: []*storage.SelectHints{
				{Start: 80000, End: 500000, Range: 120000, Func: "rate", Step: 1000},
			},
		}, {
			query: "rate(foo[2m] offset 2m)", start: 300000, end: 500000,
			expected: []*storage.SelectHints{
				{Start: 60000, End: 380000, Range: 120000, Func: "rate", Step: 1000},
			},
		}, {
			query: "rate(foo[2m:1s])", start: 300000, end: 500000,
			expected: []*storage.SelectHints{
				{Start: 175000, End: 500000, Func: "rate", Step: 1000},
			},
		}, {
			query: "count_over_time(foo[2m:1s])", start: 300000, end: 500000,
			expected: []*storage.SelectHints{
				{Start: 175000, End: 500000, Func: "count_over_time", Step: 1000},
			},
		}, {
			query: "count_over_time(foo[2m:1s] offset 10s)", start: 300000, end: 500000,
			expected: []*storage.SelectHints{
				{Start: 165000, End: 490000, Func: "count_over_time", Step: 1000},
			},
		}, {
			query: "count_over_time(foo[2m:1s] @ 300)", start: 200000, end: 500000,
			expected: []*storage.SelectHints{
				{Start: 175000, End: 300000, Func: "count_over_time", Step: 1000},
			},
		}, {
			query: "count_over_time(foo[2m:1s] @ 200)", start: 200000, end: 500000,
			expected: []*storage.SelectHints{
				{Start: 75000, End: 200000, Func: "count_over_time", Step: 1000},
			},
		}, {
			query: "count_over_time(foo[2m:1s] @ 100)", start: 200000, end: 500000,
			expected: []*storage.SelectHints{
				{Start: -25000, End: 100000, Func: "count_over_time", Step: 1000},
			},
		}, {
			query: "count_over_time((foo offset 10s)[2m:1s] offset 10s)", start: 300000, end: 500000,
			expected: []*storage.SelectHints{
				{Start: 155000, End: 480000, Func: "count_over_time", Step: 1000},
			},
		}, {
			// When the @ is on the vector selector, the enclosing subquery parameters
			// don't affect the hint ranges.
			query: "count_over_time((foo @ 200 offset 10s)[2m:1s] offset 10s)", start: 300000, end: 500000,
			expected: []*storage.SelectHints{
				{Start: 185000, End: 190000, Func: "count_over_time", Step: 1000},
			},
		}, {
			// When the @ is on the vector selector, the enclosing subquery parameters
			// don't affect the hint ranges.
			query: "count_over_time((foo @ 200 offset 10s)[2m:1s] @ 100 offset 10s)", start: 300000, end: 500000,
			expected: []*storage.SelectHints{
				{Start: 185000, End: 190000, Func: "count_over_time", Step: 1000},
			},
		}, {
			query: "count_over_time((foo offset 10s)[2m:1s] @ 100 offset 10s)", start: 300000, end: 500000,
			expected: []*storage.SelectHints{
				{Start: -45000, End: 80000, Func: "count_over_time", Step: 1000},
			},
		}, {
			query: "sum by (dim1) (foo)", start: 10000,
			expected: []*storage.SelectHints{
				{Start: 5000, End: 10000, Func: "sum", By: true, Grouping: []string{"dim1"}},
			},
		}, {
			query: "sum without (dim1) (foo)", start: 10000,
			expected: []*storage.SelectHints{
				{Start: 5000, End: 10000, Func: "sum", Grouping: []string{"dim1"}},
			},
		}, {
			query: "sum by (dim1) (avg_over_time(foo[1s]))", start: 10000,
			expected: []*storage.SelectHints{
				{Start: 9000, End: 10000, Func: "avg_over_time", Range: 1000},
			},
		}, {
			query: "sum by (dim1) (max by (dim2) (foo))", start: 10000,
			expected: []*storage.SelectHints{
				{Start: 5000, End: 10000, Func: "max", By: true, Grouping: []string{"dim2"}},
			},
		}, {
			query: "(max by (dim1) (foo))[5s:1s]", start: 10000,
			expected: []*storage.SelectHints{
				{Start: 0, End: 10000, Func: "max", By: true, Grouping: []string{"dim1"}, Step: 1000},
			},
		}, {
			query: "(sum(http_requests{group=~\"p.*\"})+max(http_requests{group=~\"c.*\"}))[20s:5s]", start: 120000,
			expected: []*storage.SelectHints{
				{Start: 95000, End: 120000, Func: "sum", By: true, Step: 5000},
				{Start: 95000, End: 120000, Func: "max", By: true, Step: 5000},
			},
		}, {
			query: "foo @ 50 + bar @ 250 + baz @ 900", start: 100000, end: 500000,
			expected: []*storage.SelectHints{
				{Start: 45000, End: 50000, Step: 1000},
				{Start: 245000, End: 250000, Step: 1000},
				{Start: 895000, End: 900000, Step: 1000},
			},
		}, {
			query: "foo @ 50 + bar + baz @ 900", start: 100000, end: 500000,
			expected: []*storage.SelectHints{
				{Start: 45000, End: 50000, Step: 1000},
				{Start: 95000, End: 500000, Step: 1000},
				{Start: 895000, End: 900000, Step: 1000},
			},
		}, {
			query: "rate(foo[2s] @ 50) + bar @ 250 + baz @ 900", start: 100000, end: 500000,
			expected: []*storage.SelectHints{
				{Start: 48000, End: 50000, Step: 1000, Func: "rate", Range: 2000},
				{Start: 245000, End: 250000, Step: 1000},
				{Start: 895000, End: 900000, Step: 1000},
			},
		}, {
			query: "rate(foo[2s:1s] @ 50) + bar + baz", start: 100000, end: 500000,
			expected: []*storage.SelectHints{
				{Start: 43000, End: 50000, Step: 1000, Func: "rate"},
				{Start: 95000, End: 500000, Step: 1000},
				{Start: 95000, End: 500000, Step: 1000},
			},
		}, {
			query: "rate(foo[2s:1s] @ 50) + bar + rate(baz[2m:1s] @ 900 offset 2m) ", start: 100000, end: 500000,
			expected: []*storage.SelectHints{
				{Start: 43000, End: 50000, Step: 1000, Func: "rate"},
				{Start: 95000, End: 500000, Step: 1000},
				{Start: 655000, End: 780000, Step: 1000, Func: "rate"},
			},
		}, { // Hints are based on the inner most subquery timestamp.
			query: `sum_over_time(sum_over_time(metric{job="1"}[100s])[100s:25s] @ 50)[3s:1s] @ 3000`, start: 100000,
			expected: []*storage.SelectHints{
				{Start: -150000, End: 50000, Range: 100000, Func: "sum_over_time", Step: 25000},
			},
		}, { // Hints are based on the inner most subquery timestamp.
			query: `sum_over_time(sum_over_time(metric{job="1"}[100s])[100s:25s] @ 3000)[3s:1s] @ 50`,
			expected: []*storage.SelectHints{
				{Start: 2800000, End: 3000000, Range: 100000, Func: "sum_over_time", Step: 25000},
			},
		},
	} {
		t.Run(tc.query, func(t *testing.T) {
			opts := promql.EngineOpts{
				Logger:           nil,
				Reg:              nil,
				MaxSamples:       10,
				Timeout:          10 * time.Second,
				LookbackDelta:    5 * time.Second,
				EnableAtModifier: true,
			}

			ng := engine.New(engine.Opts{EngineOpts: opts})
			hintsRecorder := &hintRecordingQuerier{}
			queryable := &storage.MockQueryable{MockQuerier: hintsRecorder}

			var (
				query promql.Query
				err   error
			)
			if tc.end == 0 {
				query, err = ng.NewInstantQuery(queryable, nil, tc.query, timestamp.Time(tc.start))
			} else {
				query, err = ng.NewRangeQuery(queryable, nil, tc.query, timestamp.Time(tc.start), timestamp.Time(tc.end), time.Second)
			}
			testutil.Ok(t, err)

			res := query.Exec(context.Background())
			testutil.Ok(t, res.Err)

			testutil.Equals(t, tc.expected, hintsRecorder.hints)
		})
	}
}

func TestFallback(t *testing.T) {
	start := time.Unix(0, 0)
	end := time.Unix(120, 0)
	step := time.Second * 30

	// TODO(fpetkovski): Update this expression once we add support for sort_desc.
	query := `sort_desc(http_requests_total{pod="nginx-1"})`
	load := `load 30s
				http_requests_total{pod="nginx-1"} 1+1x1
				http_requests_total{pod="nginx-2"} 1+2x40`

	test, err := promql.NewTest(t, load)
	testutil.Ok(t, err)
	defer test.Close()

	testutil.Ok(t, test.Run())

	for _, disableFallback := range []bool{true, false} {
		t.Run(fmt.Sprintf("disableFallback=%t", disableFallback), func(t *testing.T) {
			opts := promql.EngineOpts{
				Timeout:    2 * time.Second,
				MaxSamples: math.MaxInt64,
			}
			newEngine := engine.New(engine.Opts{DisableFallback: disableFallback, EngineOpts: opts})
			q1, err := newEngine.NewRangeQuery(test.Storage(), nil, query, start, end, step)
			if disableFallback {
				testutil.NotOk(t, err)
			} else {
				testutil.Ok(t, err)
				newResult := q1.Exec(context.Background())
				testutil.Ok(t, newResult.Err)
			}
		})
	}
}

func storageWithSeries(series ...storage.Series) *storage.MockQueryable {
	return &storage.MockQueryable{
		MockQuerier: &storage.MockQuerier{
			SelectMockFunction: func(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
				result := make([]storage.Series, 0)
				for _, s := range series {
				loopMatchers:
					for _, m := range matchers {
						for _, l := range s.Labels() {
							if m.Name == l.Name && m.Matches(l.Value) {
								result = append(result, s)
								break loopMatchers
							}
						}
					}
				}
				return newTestSeriesSet(result...)
			},
		},
	}
}

type mockSeries struct {
	labels     []string
	timestamps []int64
	values     []float64
}

func newMockSeries(labels []string, timestamps []int64, values []float64) *mockSeries {
	return &mockSeries{labels: labels, timestamps: timestamps, values: values}
}

func (m mockSeries) Labels() labels.Labels {
	return labels.FromStrings(m.labels...)
}

func (m mockSeries) Iterator() chunkenc.Iterator {
	return &mockIterator{
		i:          -1,
		timestamps: m.timestamps,
		values:     m.values,
	}
}

type mockIterator struct {
	i          int
	timestamps []int64
	values     []float64
}

func (m *mockIterator) Next() chunkenc.ValueType {
	m.i++
	if m.i >= len(m.values) {
		return chunkenc.ValNone
	}

	return chunkenc.ValFloat
}

func (m *mockIterator) Seek(t int64) chunkenc.ValueType {
	for {
		next := m.Next()
		if next == chunkenc.ValNone {
			return chunkenc.ValNone
		}

		if m.AtT() >= t {
			return next
		}
	}
}

func (m *mockIterator) At() (int64, float64) {
	return m.timestamps[m.i], m.values[m.i]
}

func (m *mockIterator) AtHistogram() (int64, *histogram.Histogram) { return 0, nil }

func (m *mockIterator) AtFloatHistogram() (int64, *histogram.FloatHistogram) { return 0, nil }

func (m *mockIterator) AtT() int64 { return m.timestamps[m.i] }

func (m *mockIterator) Err() error { return nil }

type testSeriesSet struct {
	i      int
	series []storage.Series
}

func newTestSeriesSet(series ...storage.Series) storage.SeriesSet {
	return &testSeriesSet{
		i:      -1,
		series: series,
	}
}

func (s *testSeriesSet) Next() bool                 { s.i++; return s.i < len(s.series) }
func (s *testSeriesSet) At() storage.Series         { return s.series[s.i] }
func (s *testSeriesSet) Err() error                 { return nil }
func (s *testSeriesSet) Warnings() storage.Warnings { return nil }

type slowSeries struct{}

func (d slowSeries) Labels() labels.Labels       { return labels.FromStrings("foo", "bar") }
func (d slowSeries) Iterator() chunkenc.Iterator { return &slowIterator{} }

type slowIterator struct {
	ts int64
}

func (d *slowIterator) AtHistogram() (int64, *histogram.Histogram) {
	panic("not implemented")
}

func (d *slowIterator) AtFloatHistogram() (int64, *histogram.FloatHistogram) {
	panic("not implemented")
}

func (d *slowIterator) AtT() int64 {
	return d.ts
}

func (d *slowIterator) At() (int64, float64) {
	return d.ts, 1
}

func (d *slowIterator) Next() chunkenc.ValueType {
	<-time.After(10 * time.Millisecond)
	d.ts += 30 * 1000
	return chunkenc.ValFloat
}

func (d *slowIterator) Seek(t int64) chunkenc.ValueType {
	<-time.After(10 * time.Millisecond)
	d.ts = t
	return chunkenc.ValFloat
}
func (d *slowIterator) Err() error { return nil }

type mockRuntimeErr struct{}

func (m *mockRuntimeErr) Error() string {
	return "panic!"
}

func (m *mockRuntimeErr) RuntimeError() {
}

func TestEngineRecoversFromPanic(t *testing.T) {
	t.Parallel()

	querier := &storage.MockQueryable{
		MockQuerier: &storage.MockQuerier{
			SelectMockFunction: func(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
				panic(runtime.Error(&mockRuntimeErr{}))
			},
		},
	}
	t.Run("instant", func(t *testing.T) {
		newEngine := engine.New(engine.Opts{
			DisableFallback: true,
		})
		q, err := newEngine.NewInstantQuery(querier, nil, "somequery", time.Time{})
		testutil.Ok(t, err)

		r := q.Exec(context.Background())
		testutil.Assert(t, r.Err.Error() == "unexpected error: panic!")
	})

	t.Run("range", func(t *testing.T) {
		newEngine := engine.New(engine.Opts{
			DisableFallback: true,
		})
		q, err := newEngine.NewRangeQuery(querier, nil, "somequery", time.Time{}, time.Time{}, 42)
		testutil.Ok(t, err)

		r := q.Exec(context.Background())
		testutil.Assert(t, r.Err.Error() == "unexpected error: panic!")
	})
}

func sortByLabels(r *promql.Result) {
	switch r.Value.Type() {
	case parser.ValueTypeVector:
		m, _ := r.Vector()
		sort.Sort(samplesByLabels(m))
		r.Value = m
	case parser.ValueTypeMatrix:
		m, _ := r.Matrix()
		sort.Sort(seriesByLabels(m))
		r.Value = m
	}
}

type seriesByLabels []promql.Series

func (b seriesByLabels) Len() int           { return len(b) }
func (b seriesByLabels) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b seriesByLabels) Less(i, j int) bool { return labels.Compare(b[i].Metric, b[j].Metric) < 0 }

type samplesByLabels []promql.Sample

func (b samplesByLabels) Len() int           { return len(b) }
func (b samplesByLabels) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b samplesByLabels) Less(i, j int) bool { return labels.Compare(b[i].Metric, b[j].Metric) < 0 }
