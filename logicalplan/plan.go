// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package logicalplan

import (
	"fmt"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
)

var DefaultOptimizers = []Optimizer{
	SortMatchers{},
	MergeSelectsOptimizer{},
}

type Plan interface {
	Optimize(...Optimizer)
	OptimizationsApplied() []string

	PreOptimizationExpr() parser.Expr
	Expr() parser.Expr
}

type Optimizer interface {
	Optimize(parser.Expr, *Log) parser.Expr
}

type plan struct {
	initial string
	expr    parser.Expr
	optLog  *Log
}

func New(expr parser.Expr, mint, maxt time.Time) Plan {
	expr = promql.PreprocessExpr(expr, mint, maxt)
	setOffsetForAtModifier(mint.UnixMilli(), expr)

	return &plan{
		initial: expr.String(),
		expr:    expr,
		optLog:  &Log{},
	}
}

func (p *plan) OptimizationsApplied() []string {
	ret := make([]string, 0, len(p.optLog.Elems()))
	for _, e := range p.optLog.Elems() {
		ret = append(ret, fmt.Sprintf("Logical Optimization -> %v", e))
	}
	return ret
}

type Log struct {
	l []string
}

func (l *Log) Addf(tmpl string, args ...interface{}) {
	if l == nil {
		return
	}
	l.l = append(l.l, fmt.Sprintf(tmpl, args...))
}

func (l *Log) Elems() []string {
	return l.l
}

func (p *plan) Optimize(optimizers ...Optimizer) {
	for _, o := range optimizers {
		p.expr = o.Optimize(p.expr, p.optLog)
	}
}

func (p *plan) PreOptimizationExpr() parser.Expr {
	e, _ := parser.ParseExpr(p.initial)
	return e
}

func (p *plan) Expr() parser.Expr {
	return p.expr
}

func traverse(expr *parser.Expr, transform func(*parser.Expr)) {
	switch node := (*expr).(type) {
	case *parser.StepInvariantExpr:
		transform(&node.Expr)
	case *parser.VectorSelector:
		transform(expr)
	case *parser.MatrixSelector:
		transform(&node.VectorSelector)
	case *parser.AggregateExpr:
		traverse(&node.Expr, transform)
	case *parser.Call:
		for _, n := range node.Args {
			traverse(&n, transform)
		}
	case *parser.BinaryExpr:
		traverse(&node.LHS, transform)
		traverse(&node.RHS, transform)
	case *parser.UnaryExpr:
		traverse(&node.Expr, transform)
	case *parser.ParenExpr:
		traverse(&node.Expr, transform)
	case *parser.SubqueryExpr:
		traverse(&node.Expr, transform)
	}
}

// Copy from https://github.com/prometheus/prometheus/blob/v2.39.1/promql/engine.go#L2658.
func setOffsetForAtModifier(evalTime int64, expr parser.Expr) {
	getOffset := func(ts *int64, originalOffset time.Duration, path []parser.Node) time.Duration {
		if ts == nil {
			return originalOffset
		}
		// TODO: support subquery.

		offsetForTs := time.Duration(evalTime-*ts) * time.Millisecond
		offsetDiff := offsetForTs
		return originalOffset + offsetDiff
	}

	parser.Inspect(expr, func(node parser.Node, path []parser.Node) error {
		switch n := node.(type) {
		case *parser.VectorSelector:
			n.Offset = getOffset(n.Timestamp, n.OriginalOffset, path)

		case *parser.MatrixSelector:
			vs := n.VectorSelector.(*parser.VectorSelector)
			vs.Offset = getOffset(vs.Timestamp, vs.OriginalOffset, path)

		case *parser.SubqueryExpr:
			n.Offset = getOffset(n.Timestamp, n.OriginalOffset, path)
		}
		return nil
	})
}
