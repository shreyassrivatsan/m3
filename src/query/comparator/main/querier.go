// Copyright (c) 2018 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package main

import (
	"context"
	"math/rand"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3"
)

var _ m3.Querier = (*querier)(nil)

type querier struct {
	encoderPool   encoding.EncoderPool
	iteratorPools encoding.IteratorPools
}

func buildDatapoints(
	seed int64,
	start time.Time,
	blockSize time.Duration,
	resolution time.Duration,
) []ts.Datapoint {
	rand.Seed(seed)
	numPoints := int(blockSize / resolution)
	dps := make([]ts.Datapoint, 0, numPoints)
	for i := 0; i < numPoints; i++ {
		dps = append(dps, ts.Datapoint{
			Timestamp: start.Add(resolution * time.Duration(i)),
			Value:     rand.Float64(),
		})
	}

	return dps
}

func (q *querier) buildOptions(
	start time.Time,
	blockSize time.Duration,
) iteratorOptions {
	return iteratorOptions{
		start:         start,
		blockSize:     blockSize,
		encoderPool:   q.encoderPool,
		iteratorPools: q.iteratorPools,
	}
}

// FetchCompressed fetches timeseries data based on a query.
func (q *querier) FetchCompressed(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (encoding.SeriesIterators, m3.Cleanup, error) {
	var (
		blockSize  = time.Hour * 12
		resolution = time.Minute

		start = time.Now().Add(time.Hour * 24 * -7).Truncate(blockSize)
		opts  = q.buildOptions(start, blockSize)
		tags  = map[string]string{"__name__": "quail"}

		dp  = buildDatapoints(query.Start.Unix(), start, blockSize, resolution)
		dps = [][]ts.Datapoint{dp}
	)

	iter, err := buildIterator(dps, tags, opts)
	if err != nil {
		return nil, nil, err
	}

	iters := encoding.NewSeriesIterators([]encoding.SeriesIterator{iter}, nil)
	cleanup := func() error {
		iters.Close()
		return nil
	}

	return iters, cleanup, nil
}

// SearchCompressed fetches matching tags based on a query.
func (q *querier) SearchCompressed(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) ([]m3.MultiTagResult, m3.Cleanup, error) {
	return nil, nil, nil
}

// CompleteTagsCompressed returns autocompleted tag results.
func (q *querier) CompleteTagsCompressed(
	ctx context.Context,
	query *storage.CompleteTagsQuery,
	options *storage.FetchOptions,
) (*storage.CompleteTagsResult, error) {
	return nil, nil
}
