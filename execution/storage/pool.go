// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package storage

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/cespare/xxhash/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

var sep = []byte{'\xff'}

type SelectorPool struct {
	selectors map[uint64]SeriesSelector

	queryable storage.Queryable
}

func NewSelectorPool(queryable storage.Queryable) *SelectorPool {
	return &SelectorPool{
		selectors: make(map[uint64]SeriesSelector),
		queryable: queryable,
	}
}

func (p *SelectorPool) GetSelector(mint, maxt, step int64, matchers []*labels.Matcher, hints storage.SelectHints) SeriesSelector {
	key := hashMatchers(matchers, mint, maxt, hints)
	if _, ok := p.selectors[key]; !ok {
		p.selectors[key] = newShardedStorageSeriesSelector(p.queryable, mint, maxt, step, matchers, hints)
	}
	return p.selectors[key]
}

func (p *SelectorPool) GetFilteredSelector(mint, maxt, step int64, matchers, filters []*labels.Matcher, hints storage.SelectHints) SeriesSelector {
	key := hashMatchers(matchers, mint, maxt, hints)
	if _, ok := p.selectors[key]; !ok {
		p.selectors[key] = newShardedStorageSeriesSelector(p.queryable, mint, maxt, step, matchers, hints)
	}

	return NewFilteredSelector(p.selectors[key], NewFilter(filters))
}

func hashMatchers(matchers []*labels.Matcher, mint, maxt int64, hints storage.SelectHints) uint64 {
	sb := xxhash.New()
	for _, m := range matchers {
		writeMatcher(sb, m)
	}
	writeInt64(sb, mint)
	writeInt64(sb, maxt)
	writeInt64(sb, hints.Step)
	writeString(sb, hints.Func)
	writeString(sb, strings.Join(hints.Grouping, ";"))
	writeBool(sb, hints.By)

	key := sb.Sum64()
	return key
}

func writeMatcher(sb *xxhash.Digest, m *labels.Matcher) {
	writeString(sb, m.Name)
	writeString(sb, strconv.Itoa(int(m.Type)))
	writeString(sb, m.Value)
}

func writeInt64(sb *xxhash.Digest, val int64) {
	_, _ = sb.WriteString(fmt.Sprintf("%d", val))
	_, _ = sb.Write(sep)
}

func writeString(sb *xxhash.Digest, val string) {
	_, _ = sb.WriteString(val)
	_, _ = sb.Write(sep)
}

func writeBool(sb *xxhash.Digest, val bool) {
	_, _ = sb.WriteString(fmt.Sprintf("%t", val))
	_, _ = sb.Write(sep)
}
