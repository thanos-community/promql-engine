// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package engine

import (
	"context"
	"fmt"
	"io"
	"math"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/efficientgo/core/errors"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/stats"
	v1 "github.com/prometheus/prometheus/web/api/v1"

	"github.com/thanos-community/promql-engine/execution"
	"github.com/thanos-community/promql-engine/execution/model"
	"github.com/thanos-community/promql-engine/execution/parse"
	"github.com/thanos-community/promql-engine/logicalplan"
)

type QueryType int

const (
	InstantQuery QueryType = 1
	RangeQuery   QueryType = 2
)

type Opts struct {
	promql.EngineOpts

	// DisableOptimizers disables Query optimizations using logicalPlan.DefaultOptimizers.
	DisableOptimizers bool

	// DisableFallback enables mode where engine returns error if some expression of feature is not yet implemented
	// in the new engine, instead of falling back to prometheus engine.
	DisableFallback bool
}

func New(opts Opts) v1.QueryEngine {
	if opts.Logger == nil {
		opts.Logger = log.NewNopLogger()
	}
	if opts.LookbackDelta == 0 {
		opts.LookbackDelta = 5 * time.Minute
		level.Debug(opts.Logger).Log("msg", "lookback delta is zero, setting to default value", "value", 5*time.Minute)
	}

	return &compatibilityEngine{
		prom: promql.NewEngine(opts.EngineOpts),
		queries: promauto.With(opts.Reg).NewCounterVec(
			prometheus.CounterOpts{
				Name: "promql_engine_queries_total",
				Help: "Number of PromQL queries.",
			}, []string{"fallback"},
		),

		disableFallback:   opts.DisableFallback,
		disableOptimizers: opts.DisableOptimizers,
		logger:            opts.Logger,
		lookbackDelta:     opts.LookbackDelta,
	}
}

type compatibilityEngine struct {
	prom    *promql.Engine
	queries *prometheus.CounterVec

	disableFallback   bool
	disableOptimizers bool
	logger            log.Logger
	lookbackDelta     time.Duration
}

func (e *compatibilityEngine) SetQueryLogger(l promql.QueryLogger) {
	e.prom.SetQueryLogger(l)
}

func (e *compatibilityEngine) NewInstantQuery(q storage.Queryable, opts *promql.QueryOpts, qs string, ts time.Time) (promql.Query, error) {
	expr, err := parser.ParseExpr(qs)
	if err != nil {
		return nil, err
	}

	lplan := logicalplan.New(expr, ts, ts)
	if !e.disableOptimizers {
		lplan.Optimize(logicalplan.DefaultOptimizers...)
	}

	exec, err := execution.New(lplan, q, ts, ts, 0, e.lookbackDelta)
	if e.triggerFallback(err) {
		e.queries.WithLabelValues("true").Inc()
		return e.prom.NewInstantQuery(q, opts, qs, ts)
	}
	e.queries.WithLabelValues("false").Inc()
	if err != nil {
		return nil, err
	}

	return &compatibilityQuery{
		Query:  &Query{execPlan: exec},
		engine: e,
		expr:   expr,
		ts:     ts,
		t:      InstantQuery,
	}, nil
}

func (e *compatibilityEngine) NewRangeQuery(q storage.Queryable, opts *promql.QueryOpts, qs string, start, end time.Time, step time.Duration) (promql.Query, error) {
	expr, err := parser.ParseExpr(qs)
	if err != nil {
		return nil, err
	}

	// Use same check as Prometheus for range queries.
	if expr.Type() != parser.ValueTypeVector && expr.Type() != parser.ValueTypeScalar {
		return nil, errors.Newf("invalid expression type %q for range Query, must be Scalar or instant Vector", parser.DocumentedType(expr.Type()))
	}

	lplan := logicalplan.New(expr, start, end)
	if !e.disableOptimizers {
		lplan.Optimize(logicalplan.DefaultOptimizers...)
	}

	exec, err := execution.New(lplan, q, start, end, step, e.lookbackDelta)
	if e.triggerFallback(err) {
		e.queries.WithLabelValues("true").Inc()
		return e.prom.NewRangeQuery(q, opts, qs, start, end, step)
	}
	e.queries.WithLabelValues("false").Inc()
	if err != nil {
		return nil, err
	}

	return &compatibilityQuery{
		Query:  &Query{execPlan: exec},
		engine: e,
		expr:   expr,
		t:      RangeQuery,
	}, nil
}

type Debuggable interface {
	Explain() string
}

type Query struct {
	execPlan execution.Plan
}

// Explain returns human-readable explanation of the created execution plan.
func (q *Query) Explain() string {
	str := strings.Builder{}
	str.WriteString("EXPLAIN:\n")
	opts := q.execPlan.OptimizationsApplied()
	if len(opts) > 0 {
		str.WriteString("PromQL execution plan before optimization:\n")
		explain(&str, q.execPlan.PreOptimizationOperator(), "", "")
		for _, o := range opts {
			str.WriteString(o)
		}
	}

	str.WriteString("Final PromQL execution plan:\n")
	explain(&str, q.execPlan.Operator(), "", "")
	return str.String()
}

func (q *Query) Profile() {
	// TODO(bwplotka): Return profile.
}

type compatibilityQuery struct {
	*Query
	engine *compatibilityEngine
	expr   parser.Expr
	ts     time.Time // Empty for range queries.
	t      QueryType

	cancel context.CancelFunc
}

func (q *compatibilityQuery) Exec(ctx context.Context) (ret *promql.Result) {
	// Handle case with strings early on as this does not need us to process samples.
	// TODO(saswatamcode): Modify models.StepVector to support all types and check during executor creation.
	ret = &promql.Result{
		Value: promql.Vector{},
	}
	defer recoverEngine(q.engine.logger, q.expr, &ret.Err)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	q.cancel = cancel

	op := q.Query.execPlan.Operator()
	resultSeries, err := op.Series(ctx)
	if err != nil {
		return newErrResult(ret, err)
	}

	series := make([]promql.Series, len(resultSeries))
	for i := 0; i < len(resultSeries); i++ {
		series[i].Metric = resultSeries[i]
	}
loop:
	for {
		select {
		case <-ctx.Done():
			return newErrResult(ret, ctx.Err())
		default:
			r, err := op.Next(ctx)
			if err != nil {
				return newErrResult(ret, err)
			}
			if r == nil {
				break loop
			}

			// Case where Series call might return nil, but samples are present.
			// For example scalar(http_request_total) where http_request_total has multiple values.
			if len(resultSeries) == 0 && len(r) != 0 {
				numSeries := 0
				for i := range r {
					numSeries += len(r[i].Samples)
				}

				series = make([]promql.Series, numSeries)

				for _, vector := range r {
					for i := range vector.Samples {
						series[i].Points = append(series[i].Points, promql.Point{
							T: vector.T,
							V: vector.Samples[i],
						})
					}
					op.GetPool().PutStepVector(vector)
				}
				op.GetPool().PutVectors(r)
				continue
			}

			for _, vector := range r {
				for i, s := range vector.SampleIDs {
					if len(series[s].Points) == 0 {
						series[s].Points = make([]promql.Point, 0, 121) // Typically 1h of data.
					}
					series[s].Points = append(series[s].Points, promql.Point{
						T: vector.T,
						V: vector.Samples[i],
					})
				}
				op.GetPool().PutStepVector(vector)
			}
			op.GetPool().PutVectors(r)
		}
	}

	// For range Query we expect always a Matrix value type.
	if q.t == RangeQuery {
		resultMatrix := make(promql.Matrix, 0, len(series))
		for _, s := range series {
			if len(s.Points) == 0 {
				continue
			}
			resultMatrix = append(resultMatrix, s)
		}
		sort.Sort(resultMatrix)
		ret.Value = resultMatrix
		return ret
	}

	var result parser.Value
	switch q.expr.Type() {
	case parser.ValueTypeMatrix:
		result = promql.Matrix(series)
	case parser.ValueTypeVector:
		// Convert matrix with one value per series into vector.
		vector := make(promql.Vector, 0, len(resultSeries))
		for i := range series {
			if len(series[i].Points) == 0 {
				continue
			}
			// Point might have a different timestamp, force it to the evaluation
			// timestamp as that is when we ran the evaluation.
			vector = append(vector, promql.Sample{
				Metric: series[i].Metric,
				Point: promql.Point{
					V: series[i].Points[0].V,
					T: q.ts.UnixMilli(),
				},
			})
		}
		result = vector
	case parser.ValueTypeScalar:
		v := math.NaN()
		if len(series) != 0 {
			v = series[0].Points[0].V
		}
		result = promql.Scalar{V: v, T: q.ts.UnixMilli()}
	default:
		panic(errors.Newf("new.Engine.exec: unexpected expression type %q", q.expr.Type()))
	}

	ret.Value = result
	return ret
}

func newErrResult(r *promql.Result, err error) *promql.Result {
	if r == nil {
		r = &promql.Result{}
	}
	if r.Err == nil && err != nil {
		r.Err = err
	}
	return r
}

func (q *compatibilityQuery) Statement() parser.Statement { return nil }

func (q *compatibilityQuery) Stats() *stats.Statistics { return &stats.Statistics{} }

func (q *compatibilityQuery) Close() { q.Cancel() }

func (q *compatibilityQuery) String() string { return q.expr.String() }

func (q *compatibilityQuery) Cancel() {
	if q.cancel != nil {
		q.cancel()
		q.cancel = nil
	}
}

func (e *compatibilityEngine) triggerFallback(err error) bool {
	if e.disableFallback {
		return false
	}

	return errors.Is(err, parse.ErrNotSupportedExpr) || errors.Is(err, parse.ErrNotImplemented)
}

func recoverEngine(logger log.Logger, expr parser.Expr, errp *error) {
	e := recover()
	if e == nil {
		return
	}

	switch err := e.(type) {
	case runtime.Error:
		// Print the stack trace but do not inhibit the running application.
		buf := make([]byte, 64<<10)
		buf = buf[:runtime.Stack(buf, false)]

		level.Error(logger).Log("msg", "runtime panic in engine", "expr", expr.String(), "err", e, "stacktrace", string(buf))
		*errp = fmt.Errorf("unexpected error: %w", err)
	}
}

func explain(w io.Writer, o model.VectorOperator, indent, indentNext string) {
	me, next := o.Explain()
	_, _ = w.Write([]byte(indent))
	_, _ = w.Write([]byte(me))
	if len(next) == 0 {
		_, _ = w.Write([]byte("\n"))
		return
	}

	_, _ = w.Write([]byte(":\n"))

	for i, n := range next {
		if i == len(next)-1 {
			explain(w, n, indentNext+"└──", indentNext+"   ")
		} else {
			explain(w, n, indentNext+"├──", indentNext+"│  ")
		}
	}
}
