package tsz

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"time"

	"code.uber.internal/infra/memtsdb"
	"code.uber.internal/infra/memtsdb/encoding"
)

const (
	maxBufferSize = 8
)

// iterator provides an interface for clients to incrementally
// read datapoints off of an encoded stream.
type iterator struct {
	is *istream
	tu time.Duration // time unit

	// internal bookkeeping
	nt   int64               // current time
	dt   int64               // current time delta
	vb   uint64              // current value
	xor  uint64              // current xor
	done bool                // has reached the end
	err  error               // current error
	ant  encoding.Annotation // current annotation
	buf  [maxBufferSize]byte // a small buffer to avoid repeated allocation
}

func newIterator(reader io.Reader, timeUnit time.Duration) encoding.Iterator {
	return &iterator{
		is: newIStream(reader),
		tu: timeUnit,
	}
}

// Next moves to the next item
func (it *iterator) Next() bool {
	if !it.hasNext() {
		return false
	}
	it.ant = nil
	if it.nt == 0 {
		it.readFirstTimestamp()
		it.readFirstValue()
	} else {
		it.readNextTimestamp()
		it.readNextValue()
	}
	return it.hasNext()
}

func (it *iterator) readFirstTimestamp() {
	it.nt = int64(it.readBits(64))
	it.readNextTimestamp()
}

func (it *iterator) readFirstValue() {
	it.vb = it.readBits(64)
	it.xor = it.vb
}

func (it *iterator) readNextTimestamp() {
	dod := it.readDeltaOfDelta()
	it.dt += dod
	it.nt += it.dt
}

func (it *iterator) readDeltaOfDelta() int64 {
	cb := it.readBits(zeroDoDRange.numOpcodeBits)
	if cb == zeroDoDRange.opcode {
		return 0
	}
	for i := 0; i < len(dodRanges); i++ {
		cb = (it.readBits(1) << uint(i+1)) | cb
		if cb == dodRanges[i].opcode {
			return signExtend(it.readBits(dodRanges[i].numDoDBits), dodRanges[i].numDoDBits)
		}
	}
	dod := signExtend(it.readBits(defaultDoDRange.numDoDBits), defaultDoDRange.numDoDBits)
	if !it.hasError() {
		if dod == int64(annotationMarker) {
			it.readAnnotation()
			return it.readDeltaOfDelta()
		}
		if dod == int64(eosMarker) {
			it.done = true
		}
	}
	return dod
}

func (it *iterator) readNextValue() {
	it.xor = it.readXOR()
	it.vb ^= it.xor
}

func (it *iterator) readAnnotation() {
	// NB: we add 1 here to offset the 1 we subtracted during encoding
	antLen := it.readVarint() + 1
	if antLen <= 0 {
		it.err = fmt.Errorf("unexpected annotation length %d", antLen)
		return
	}
	buf := it.buf[:]
	if antLen > maxBufferSize {
		buf = make([]byte, antLen)
	}
	for i := 0; i < antLen; i++ {
		buf[i] = byte(it.readBits(8))
	}
	it.ant = buf[:antLen]
}

func (it *iterator) readXOR() uint64 {
	cb := it.readBits(1)
	if cb == opcodeZeroValueXOR {
		return 0
	}

	cb = (it.readBits(1) << 1) | cb
	if cb == opcodeContainedValueXOR {
		previousLeading, previousTrailing := leadingAndTrailingZeros(it.xor)
		numMeaningfulBits := 64 - previousLeading - previousTrailing
		return it.readBits(numMeaningfulBits) << uint(previousTrailing)
	}

	numLeadingZeros := int(it.readBits(6))
	numMeaningfulBits := int(it.readBits(6)) + 1
	numTrailingZeros := 64 - numLeadingZeros - numMeaningfulBits
	meaningfulBits := it.readBits(numMeaningfulBits)
	return meaningfulBits << uint(numTrailingZeros)
}

func (it *iterator) readBits(numBits int) uint64 {
	if !it.hasNext() {
		return 0
	}
	var res uint64
	res, it.err = it.is.ReadBits(numBits)
	return res
}

func (it *iterator) readVarint() int {
	if !it.hasNext() {
		return 0
	}
	var res int64
	res, it.err = binary.ReadVarint(it.is)
	return int(res)
}

// Current returns the value as well as the annotation associated with the current datapoint.
// Users should not hold on to the returned Annotation object as it may get invalidated when
// the iterator calls Next().
func (it *iterator) Current() (encoding.Datapoint, encoding.Annotation) {
	return encoding.Datapoint{
		Timestamp: memtsdb.FromNormalizedTime(it.nt, it.tu),
		Value:     math.Float64frombits(it.vb),
	}, it.ant
}

// Err returns the error encountered
func (it *iterator) Err() error {
	return it.err
}

func (it *iterator) hasError() bool {
	return it.err != nil
}

func (it *iterator) isDone() bool {
	return it.done
}

func (it *iterator) hasNext() bool {
	return !it.hasError() && !it.isDone()
}
