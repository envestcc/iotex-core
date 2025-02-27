package blockutil

import (
	"math"
	"sort"
	"time"

	"github.com/pkg/errors"
)

type (
	// BlockTimeCalculator calculates block time of a given height.
	BlockTimeCalculator struct {
		getBlockInterval    getBlockIntervalFn
		getTipHeight        getTipHeightFn
		getHistoryBlockTime getHistoryblockTimeFn

		intervalChanges []*intervalChange
	}

	intervalChange struct {
		height   uint64
		interval time.Duration
	}

	getBlockIntervalFn    func(uint64) time.Duration
	getTipHeightFn        func() uint64
	getHistoryblockTimeFn func(uint64) (time.Time, error)
)

// NewBlockTimeCalculator creates a new BlockTimeCalculator.
func NewBlockTimeCalculator(getBlockInterval getBlockIntervalFn, getTipHeight getTipHeightFn, getHistoryBlockTime getHistoryblockTimeFn, intervalChanges map[uint64]time.Duration) (*BlockTimeCalculator, error) {
	if getBlockInterval == nil {
		return nil, errors.New("nil getBlockInterval")
	}
	if getTipHeight == nil {
		return nil, errors.New("nil getTipHeight")
	}
	if getHistoryBlockTime == nil {
		return nil, errors.New("nil getHistoryBlockTime")
	}
	if len(intervalChanges) == 0 {
		return nil, errors.New("empty interval changes")
	}
	changes := make([]*intervalChange, 0, len(intervalChanges))
	for height, interval := range intervalChanges {
		changes = append(changes, &intervalChange{height: height, interval: interval})
	}
	sort.Slice(changes, func(i, j int) bool {
		return changes[i].height < changes[j].height
	})
	if changes[0].height != 0 {
		return nil, errors.New("first interval change height must be 0")
	}
	return &BlockTimeCalculator{
		getBlockInterval:    getBlockInterval,
		getTipHeight:        getTipHeight,
		getHistoryBlockTime: getHistoryBlockTime,
		intervalChanges:     changes,
	}, nil
}

// CalculateBlockTime returns the block time of the given height.
// If the height is in the future, it will predict the block time according to the tip block time and interval.
// If the height is in the past, it will get the block time from indexer.
func (btc *BlockTimeCalculator) CalculateBlockTime(height uint64) (time.Time, error) {
	// get block time from indexer if height is in the past
	tipHeight := btc.getTipHeight()
	if height <= tipHeight {
		return btc.getHistoryBlockTime(height)
	}

	// predict block time according to tip block time and interval
	blockInterval := btc.getBlockInterval(tipHeight)
	blockNumer := time.Duration(height - tipHeight)
	if blockNumer > math.MaxInt64/blockInterval {
		return time.Time{}, errors.New("height overflow")
	}
	tipBlockTime, err := btc.getHistoryBlockTime(tipHeight)
	if err != nil {
		return time.Time{}, err
	}
	return tipBlockTime.Add(blockNumer * blockInterval), nil
}

// ExpectBlockHeight returns the expect block height of the given time without any block missing.
func (btc *BlockTimeCalculator) ExpectBlockHeight(base uint64, ts time.Time) (uint64, error) {
	// base must be historical block height
	tipHeight := btc.getTipHeight()
	if base > tipHeight {
		return 0, errors.Errorf("base height %d is in the future of tip height %d", base, tipHeight)
	}
	// ts must be in the future of base block time
	baseBlockTime, err := btc.getHistoryBlockTime(base)
	if err != nil {
		return 0, err
	}
	if ts.Before(baseBlockTime) {
		return 0, errors.New("time is in the past of base block time")
	}

	var (
		heights   []uint64
		intervals []time.Duration
	)
	for _, change := range btc.intervalChanges {
		heights = append(heights, change.height)
		intervals = append(intervals, change.interval)
	}
	return predictBlock(base, baseBlockTime, ts, heights, intervals), nil
}

// predictBlock predicts the block height corresponding to a given timestamp (ts)
// starting from a given base height (base) and its block time (baseBlockTime).
func predictBlock(base uint64, baseBlockTime, ts time.Time, heights []uint64, intervals []time.Duration) uint64 {
	// Find the interval where the base belongs:
	// If heights[1] exists and base < heights[1], then base is in the first interval, so use intervals[0]
	// Otherwise, iterate through heights to find the interval [heights[i], heights[i+1]) that contains base.
	idx := 0
	if len(heights) > 1 && base < heights[1] {
		idx = 0
	} else {
		// Iterate to find the correct interval where base >= heights[i] and (i+1 is out of bounds or base < heights[i+1])
		for i := 0; i < len(heights); i++ {
			// If this is the last interval, assign directly.
			if i == len(heights)-1 {
				idx = i
				break
			}
			if base >= heights[i] && base < heights[i+1] {
				idx = i
				break
			}
		}
	}

	predictedBlock := base
	predictedTime := baseBlockTime

	for {
		var segmentEnd uint64
		var segmentDuration time.Duration
		// Determine the end of the current interval: if a next boundary exists, use it; otherwise, use math.MaxUint64.
		if idx+1 < len(heights) {
			segmentEnd = heights[idx+1]
		} else {
			segmentEnd = math.MaxUint64
		}

		currentInterval := intervals[idx]
		blocksAvailable := segmentEnd - predictedBlock
		if segmentEnd == math.MaxUint64 {
			segmentDuration = time.Duration(math.MaxInt64)
		} else {
			segmentDuration = time.Duration(blocksAvailable) * currentInterval
		}

		// If the current interval duration is enough to cover ts, calculate the number of blocks to add.
		if t := predictedTime.Add(segmentDuration); !t.Before(ts) {
			remaining := ts.Sub(predictedTime)
			blocksToAdd := uint64(remaining / currentInterval)
			return predictedBlock + blocksToAdd
		}

		// Accumulate the entire duration of the current interval and move to the next.
		predictedTime = predictedTime.Add(segmentDuration)
		predictedBlock = segmentEnd

		// Move to the next interval; if idx exceeds the intervals array, continue using the last interval.
		idx++
		if idx >= len(intervals) {
			idx = len(intervals) - 1
		}
		if predictedBlock == math.MaxUint64 {
			return math.MaxUint64
		}
	}
}
