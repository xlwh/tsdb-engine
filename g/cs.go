package g

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"io"
	"math"
	"math/bits"
	"sync"
)

type Series struct {
	sync.Mutex

	T0 int64
	t  int64

	cnt float64
	sum float64
	max float64
	min float64

	bw bstream

	cntLeading  uint8
	cntTrailing uint8
	sumLeading  uint8
	sumTrailing uint8
	maxLeading  uint8
	maxTrailing uint8
	minLeading  uint8
	minTrailing uint8
	avgLeading  uint8
	avgTrailing uint8

	finished bool

	tDelta  int32
	dataCnt int32
}

func New(t0 int64) *Series {
	s := Series{
		T0:         t0,
		cntLeading: ^uint8(0),
		sumLeading: ^uint8(0),
		maxLeading: ^uint8(0),
		minLeading: ^uint8(0),
		dataCnt:    1,
	}

	s.bw.writeBits(uint64(t0), 64)

	return &s

}

func (s *Series) Bytes() []byte {
	s.Lock()
	buf := make([]byte, len(s.bw.bytes()))
	copy(buf, s.bw.bytes())
	s.Unlock()

	return buf
}

func finish(w *bstream) {
	// write an end-of-stream record
	w.writeBits(0x0f, 4)
	w.writeBits(0xffffffff, 32)
	w.writeBit(zero)
}

func (s *Series) Finish() {
	s.Lock()
	if !s.finished {
		finish(&s.bw)
		s.finished = true
	}
	s.Unlock()
}

func (s *Series) DataLen() int32 {
	s.Lock()
	defer s.Unlock()

	return s.dataCnt
}

func (s *Series) Push(t int64, cnt, sum, max, min float64) {
	s.Lock()
	defer s.Unlock()
	s.dataCnt++

	if s.t == 0 {
		// first point
		s.t = t
		s.cnt = cnt
		s.sum = sum
		s.max = max
		s.min = min
		s.tDelta = int32(t - s.T0)
		s.bw.writeBits(uint64(s.tDelta), 14)
		s.bw.writeBits(math.Float64bits(cnt), 64)
		s.bw.writeBits(math.Float64bits(sum), 64)
		s.bw.writeBits(math.Float64bits(max), 64)
		s.bw.writeBits(math.Float64bits(min), 64)
		return
	}

	tDelta := int32(t - s.t)
	dod := int32(tDelta - s.tDelta)

	switch {
	case dod == 0:
		s.bw.writeBit(zero)
	case -63 <= dod && dod <= 64:
		s.bw.writeBits(0x02, 2) // '10'
		s.bw.writeBits(uint64(dod), 7)
	case -255 <= dod && dod <= 256:
		s.bw.writeBits(0x06, 3) // '110'
		s.bw.writeBits(uint64(dod), 9)
	case -2047 <= dod && dod <= 2048:
		s.bw.writeBits(0x0e, 4) // '1110'
		s.bw.writeBits(uint64(dod), 12)
	default:
		s.bw.writeBits(0x0f, 4) // '1111'
		s.bw.writeBits(uint64(dod), 32)
	}

	cntDelta := math.Float64bits(cnt) ^ math.Float64bits(s.cnt)

	if cntDelta == 0 {
		s.bw.writeBit(zero)
	} else {
		s.bw.writeBit(one)
		cntLeading := uint8(bits.LeadingZeros64(cntDelta))
		cntTrailing := uint8(bits.TrailingZeros64(cntDelta))

		if cntLeading >= 32 {
			cntLeading = 31
		}

		if s.cntLeading != ^uint8(0) && cntLeading >= s.cntLeading && cntTrailing >= s.cntTrailing {
			s.bw.writeBit(zero)
			s.bw.writeBits(cntDelta>>s.cntTrailing, 64-int(s.cntLeading)-int(s.cntTrailing))
		} else {
			s.cntLeading, s.cntTrailing = cntLeading, cntTrailing

			s.bw.writeBit(one)
			s.bw.writeBits(uint64(cntLeading), 5)
			sigbits := 64 - cntLeading - cntTrailing
			s.bw.writeBits(uint64(sigbits), 6)
			s.bw.writeBits(cntDelta>>cntTrailing, int(sigbits))
		}
	}

	sumDelta := math.Float64bits(sum) ^ math.Float64bits(s.sum)
	if sumDelta == 0 {
		s.bw.writeBit(zero)
	} else {
		s.bw.writeBit(one)

		sumLeading := uint8(bits.LeadingZeros64(sumDelta))
		sumTrailing := uint8(bits.TrailingZeros64(sumDelta))
		if sumLeading >= 32 {
			sumLeading = 31
		}

		if s.sumLeading != ^uint8(0) && sumLeading >= s.sumLeading && sumTrailing >= s.sumTrailing {
			s.bw.writeBit(zero)
			s.bw.writeBits(sumDelta>>s.sumTrailing, 64-int(s.sumLeading)-int(s.sumTrailing))
		} else {
			s.sumLeading, s.sumTrailing = sumLeading, sumTrailing

			s.bw.writeBit(one)
			s.bw.writeBits(uint64(sumLeading), 5)
			sigbits := 64 - sumLeading - sumTrailing
			s.bw.writeBits(uint64(sigbits), 6)
			s.bw.writeBits(sumDelta>>sumTrailing, int(sigbits))
		}
	}

	maxDelta := math.Float64bits(max) ^ math.Float64bits(s.max)
	if maxDelta == 0 {
		s.bw.writeBit(zero)
	} else {
		s.bw.writeBit(one)
		maxLeading := uint8(bits.LeadingZeros64(maxDelta))
		maxTrailing := uint8(bits.TrailingZeros64(maxDelta))
		if maxLeading >= 32 {
			maxLeading = 31
		}

		if s.maxLeading != ^uint8(0) && maxLeading >= s.maxLeading && maxTrailing >= s.maxTrailing {
			s.bw.writeBit(zero)
			s.bw.writeBits(maxDelta>>s.maxTrailing, 64-int(s.maxLeading)-int(s.maxTrailing))
		} else {
			s.maxLeading, s.maxTrailing = maxLeading, maxTrailing

			s.bw.writeBit(one)
			s.bw.writeBits(uint64(maxLeading), 5)
			sigbits := 64 - maxLeading - maxTrailing
			s.bw.writeBits(uint64(sigbits), 6)
			s.bw.writeBits(maxDelta>>maxTrailing, int(sigbits))
		}
	}

	minDelta := math.Float64bits(min) ^ math.Float64bits(s.min)

	if minDelta == 0 {
		s.bw.writeBit(zero)
	} else {
		s.bw.writeBit(one)
		minLeading := uint8(bits.LeadingZeros64(minDelta))
		minTrailing := uint8(bits.TrailingZeros64(minDelta))

		if minLeading >= 32 {
			minLeading = 31
		}

		if s.minLeading != ^uint8(0) && minLeading >= s.minLeading && minTrailing >= s.minTrailing {
			s.bw.writeBit(zero)
			s.bw.writeBits(minDelta>>s.minTrailing, 64-int(s.minLeading)-int(s.minTrailing))
		} else {
			s.minLeading, s.minTrailing = minLeading, minTrailing

			s.bw.writeBit(one)
			s.bw.writeBits(uint64(minLeading), 5)
			sigbits := 64 - minLeading - minTrailing
			s.bw.writeBits(uint64(sigbits), 6)
			s.bw.writeBits(minDelta>>minTrailing, int(sigbits))
		}
	}

	s.tDelta = tDelta

	s.t = t
	s.cnt = cnt
	s.sum = sum
	s.max = max
	s.min = min
}

func (s *Series) Iter() *Iter {
	s.Lock()
	defer s.Unlock()

	w := s.bw.clone()

	finish(w)
	iter, _ := bstreamIterator(w)
	return iter
}

type Iter struct {
	T0 int64

	t int64

	cnt float64
	sum float64
	max float64
	min float64

	br          bstream
	cntLeading  uint8
	cntTrailing uint8
	sumLeading  uint8
	sumTrailing uint8
	maxLeading  uint8
	maxTrailing uint8
	minLeading  uint8
	minTrailing uint8

	finished bool

	tDelta int32
	err    error
}

func bstreamIterator(br *bstream) (*Iter, error) {

	br.count = 8

	t0, err := br.readBits(64)
	if err != nil {
		return nil, err
	}

	return &Iter{
		T0: int64(t0),
		br: *br,
	}, nil
}

// NewIterator for the series
func NewIterator(b []byte) (*Iter, error) {
	return bstreamIterator(newBReader(b))
}

func Hash(data []byte) string {
	md5Ctx := md5.New()
	md5Ctx.Write(data)
	cipherStr := md5Ctx.Sum(nil)

	return hex.EncodeToString(cipherStr)
}

func (it *Iter) Next() bool {
	if it.err != nil || it.finished {
		return false
	}

	if it.t == 0 {
		tDelta, err := it.br.readBits(14)
		if err != nil {
			it.err = err
			return false
		}
		it.tDelta = int32(tDelta)
		it.t = it.T0 + int64(it.tDelta)

		cnt, err := it.br.readBits(64)
		if err != nil {
			it.err = err
			return false
		}

		it.cnt = math.Float64frombits(cnt)
		sum, err := it.br.readBits(64)
		if err != nil {
			it.err = err
			return false
		}

		it.sum = math.Float64frombits(sum)

		max, err := it.br.readBits(64)
		if err != nil {
			it.err = err
			return false
		}

		it.max = math.Float64frombits(max)

		min, err := it.br.readBits(64)
		if err != nil {
			it.err = err
			return false
		}

		it.min = math.Float64frombits(min)
		return true
	}

	// read delta-of-delta
	var d byte
	for i := 0; i < 4; i++ {
		d <<= 1
		bit, err := it.br.readBit()
		if err != nil {
			it.err = err
			return false
		}
		if bit == zero {
			break
		}
		d |= 1
	}

	var dod int32
	var sz uint
	switch d {
	case 0x00:
	case 0x02:
		sz = 7
	case 0x06:
		sz = 9
	case 0x0e:
		sz = 12
	case 0x0f:
		bits, err := it.br.readBits(32)
		if err != nil {
			it.err = err
			return false
		}

		if bits == 0xffffffff {
			it.finished = true
			return false
		}

		dod = int32(bits)
	}

	if sz != 0 {
		bits, err := it.br.readBits(int(sz))
		if err != nil {
			it.err = err
			return false
		}
		if bits > (1 << (sz - 1)) {
			// or something
			bits = bits - (1 << sz)
		}
		dod = int32(bits)
	}

	tDelta := it.tDelta + int32(dod)

	it.tDelta = tDelta
	it.t = it.t + int64(it.tDelta)

	bit, err := it.br.readBit()
	if err != nil {
		it.err = err
		return false
	}

	if bit == zero {
		it.cnt = it.cnt
	} else {
		bit, itErr := it.br.readBit()
		if itErr != nil {
			it.err = err
			return false
		}
		if bit == zero {
			it.cntLeading, it.cntTrailing = it.cntLeading, it.cntTrailing
		} else {
			bits, err := it.br.readBits(5)
			if err != nil {
				it.err = err
				return false
			}

			it.cntLeading = uint8(bits)

			bits, err = it.br.readBits(6)
			if err != nil {
				it.err = err
				return false
			}
			mbits := uint8(bits)
			// 0 significant bits here means we overflowed and we actually need 64; see comment in encoder
			if mbits == 0 {
				mbits = 64
			}
			it.cntTrailing = 64 - it.cntLeading - mbits
		}

		mbits := int(64 - it.cntLeading - it.cntTrailing)
		bits, err := it.br.readBits(mbits)
		if err != nil {
			it.err = err
			return false
		}
		vbits := math.Float64bits(it.cnt)
		vbits ^= (bits << it.cntTrailing)
		it.cnt = math.Float64frombits(vbits)
	}

	bit, err = it.br.readBit()
	if err != nil {
		it.err = err
		return false
	}

	if bit == zero {
		it.sum = it.sum
	} else {
		bit, itErr := it.br.readBit()
		if itErr != nil {
			it.err = err
			return false
		}
		if bit == zero {
			it.sumLeading, it.sumTrailing = it.sumLeading, it.sumTrailing
		} else {
			bits, err := it.br.readBits(5)
			if err != nil {
				it.err = err
				return false
			}
			it.sumLeading = uint8(bits)

			bits, err = it.br.readBits(6)
			if err != nil {
				it.err = err
				return false
			}
			mbits := uint8(bits)
			if mbits == 0 {
				mbits = 64
			}
			it.sumTrailing = 64 - it.sumLeading - mbits
		}

		mbits := int(64 - it.sumLeading - it.sumTrailing)
		bits, err := it.br.readBits(mbits)
		if err != nil {
			it.err = err
			return false
		}
		vbits := math.Float64bits(it.sum)
		vbits ^= (bits << it.sumTrailing)
		it.sum = math.Float64frombits(vbits)
	}

	bit, err = it.br.readBit()
	if err != nil {
		it.err = err
		return false
	}

	if bit == zero {
		it.max = it.max
	} else {
		bit, itErr := it.br.readBit()
		if itErr != nil {
			it.err = err
			return false
		}
		if bit == zero {
			it.maxLeading, it.maxTrailing = it.maxLeading, it.maxTrailing
		} else {
			bits, err := it.br.readBits(5)
			if err != nil {
				it.err = err
				return false
			}
			it.maxLeading = uint8(bits)

			bits, err = it.br.readBits(6)
			if err != nil {
				it.err = err
				return false
			}
			mbits := uint8(bits)
			if mbits == 0 {
				mbits = 64
			}
			it.maxTrailing = 64 - it.maxLeading - mbits
		}

		mbits := int(64 - it.maxLeading - it.maxTrailing)
		bits, err := it.br.readBits(mbits)
		if err != nil {
			it.err = err
			return false
		}
		vbits := math.Float64bits(it.max)
		vbits ^= (bits << it.maxTrailing)
		it.max = math.Float64frombits(vbits)
	}

	bit, err = it.br.readBit()
	if err != nil {
		it.err = err
		return false
	}

	if bit == zero {
		it.min = it.min
	} else {
		bit, itErr := it.br.readBit()
		if itErr != nil {
			it.err = err
			return false
		}
		if bit == zero {
			it.minLeading, it.minTrailing = it.minLeading, it.minTrailing
		} else {
			bits, err := it.br.readBits(5)
			if err != nil {
				it.err = err
				return false
			}
			it.minLeading = uint8(bits)

			bits, err = it.br.readBits(6)
			if err != nil {
				it.err = err
				return false
			}
			mbits := uint8(bits)
			if mbits == 0 {
				mbits = 64
			}
			it.minTrailing = 64 - it.minLeading - mbits
		}

		mbits := int(64 - it.minLeading - it.minTrailing)
		bits, err := it.br.readBits(mbits)
		if err != nil {
			it.err = err
			return false
		}
		vbits := math.Float64bits(it.min)
		vbits ^= (bits << it.minTrailing)
		it.min = math.Float64frombits(vbits)
	}

	return true
}

func (it *Iter) Values() (int64, float64, float64, float64, float64) {
	return it.t, it.cnt, it.sum, it.max, it.min
}

func (it *Iter) Err() error {
	return it.err
}

type errMarshal struct {
	w   io.Writer
	r   io.Reader
	err error
}

func (em *errMarshal) write(t interface{}) {
	if em.err != nil {
		return
	}
	em.err = binary.Write(em.w, binary.BigEndian, t)
}

func (em *errMarshal) read(t interface{}) {
	if em.err != nil {
		return
	}
	em.err = binary.Read(em.r, binary.BigEndian, t)
}

func (s *Series) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	em := &errMarshal{w: buf}
	em.write(s.T0)
	em.write(s.cntLeading)
	em.write(s.sumLeading)
	em.write(s.maxLeading)
	em.write(s.minLeading)
	em.write(s.avgLeading)
	em.write(s.t)
	em.write(s.tDelta)
	em.write(s.cntTrailing)
	em.write(s.sumTrailing)
	em.write(s.maxTrailing)
	em.write(s.minTrailing)
	em.write(s.avgTrailing)
	em.write(s.cnt)
	em.write(s.sum)
	em.write(s.max)
	em.write(s.min)
	bStream, err := s.bw.MarshalBinary()
	if err != nil {
		return nil, err
	}
	em.write(bStream)
	if em.err != nil {
		return nil, em.err
	}
	return buf.Bytes(), nil
}

func (s *Series) UnmarshalBinary(b []byte) error {
	buf := bytes.NewReader(b)
	em := &errMarshal{r: buf}
	em.read(&s.T0)
	em.read(&s.cntLeading)
	em.read(&s.sumLeading)
	em.read(&s.maxLeading)
	em.read(&s.minLeading)
	em.read(&s.avgLeading)
	em.read(&s.t)
	em.read(&s.tDelta)
	em.read(&s.cntTrailing)
	em.read(&s.sumTrailing)
	em.read(&s.maxTrailing)
	em.read(&s.minTrailing)
	em.read(&s.avgTrailing)
	em.read(&s.cnt)
	em.read(&s.sum)
	em.read(&s.max)
	em.read(&s.min)
	outBuf := make([]byte, buf.Len())
	em.read(outBuf)
	err := s.bw.UnmarshalBinary(outBuf)
	if err != nil {
		return err
	}
	if em.err != nil {
		return em.err
	}
	return nil
}
