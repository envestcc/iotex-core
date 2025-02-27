package blockutil

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBlockTimeCalculator_CalculateBlockTime(t *testing.T) {
	r := require.New(t)
	interval := 5 * time.Second
	intervalFn := func(h uint64) time.Duration {
		return 5 * time.Second
	}
	tipHeight := uint64(100)
	tipHeightF := func() uint64 { return tipHeight }
	baseTime, err := time.Parse("2006-01-02T15:04:05.000Z", "2022-01-01T00:00:00.000Z")
	r.NoError(err)
	historyBlockTimeF := func(height uint64) (time.Time, error) { return baseTime.Add(time.Hour * time.Duration(height)), nil }
	btc, err := NewBlockTimeCalculator(intervalFn, tipHeightF, historyBlockTimeF, map[uint64]time.Duration{0: interval})
	r.NoError(err)

	historyWrapper := func(height uint64) time.Time {
		t, err := historyBlockTimeF(height)
		r.NoError(err)
		return t
	}
	cases := []struct {
		name   string
		height uint64
		want   time.Time
		errMsg string
	}{
		{"height is in the past", tipHeight - 1, historyWrapper(tipHeight - 1), ""},
		{"height is in the past I", tipHeight, historyWrapper(tipHeight), ""},
		{"height is in the future", tipHeight + 1, historyWrapper(tipHeight).Add(interval), ""},
		{"height is in the future I", tipHeight + 2, historyWrapper(tipHeight).Add(2 * interval), ""},
		{"height is not overflow", tipHeight + (1<<63-1)/uint64(interval), historyWrapper(tipHeight).Add((1<<63 - 1) / interval * interval), ""},
		{"height is overflow", tipHeight + (1<<63-1)/uint64(interval) + 1, time.Time{}, "height overflow"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := btc.CalculateBlockTime(c.height)
			if c.errMsg != "" {
				r.ErrorContains(err, c.errMsg)
				return
			}
			r.NoError(err)
			r.Equal(c.want, got)
		})
	}
}

func TestPredictBlock(t *testing.T) {
	heights := []uint64{0, 100, 200}
	intervals := []time.Duration{10 * time.Second, 5 * time.Second, 3 * time.Second}
	baseBlockTime := time.Now()
	cases := []struct {
		base uint64
		ts   time.Time
		want uint64
	}{
		{50, baseBlockTime.Add(5 * time.Second), 50},
		{50, baseBlockTime.Add(10 * time.Second), 51},
		{50, baseBlockTime.Add(19 * time.Second), 51},
		{50, baseBlockTime.Add(490 * time.Second), 99},
		{50, baseBlockTime.Add(499 * time.Second), 99},
		{50, baseBlockTime.Add(500 * time.Second), 100},
		{50, baseBlockTime.Add(501 * time.Second), 100},
		{50, baseBlockTime.Add(505 * time.Second), 101},
		{50, baseBlockTime.Add(509 * time.Second), 101},
		{50, baseBlockTime.Add(995 * time.Second), 199},
		{50, baseBlockTime.Add(1000 * time.Second), 200},
		{50, baseBlockTime.Add(1003 * time.Second), 201},

		{100, baseBlockTime.Add(1 * time.Second), 100},
		{100, baseBlockTime.Add(5 * time.Second), 101},
		{100, baseBlockTime.Add(99 * 5 * time.Second), 199},
		{100, baseBlockTime.Add(500 * time.Second), 200},
		{100, baseBlockTime.Add(501 * time.Second), 200},
		{100, baseBlockTime.Add(503 * time.Second), 201},

		{200, baseBlockTime.Add(1 * time.Second), 200},
		{200, baseBlockTime.Add(3 * time.Second), 201},
		{200, baseBlockTime.Add(5 * time.Second), 201},
		{200, baseBlockTime.Add(6 * time.Second), 202},
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			got := predictBlock(c.base, baseBlockTime, c.ts, heights, intervals)
			require.Equal(t, c.want, got)
		})
	}
}
