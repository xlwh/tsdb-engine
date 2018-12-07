package simple

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"io"
	"math"
	"math/bits"
	"sync"

	"errors"
	log "github.com/cihub/seelog"
)

type Series struct {
	sync.Mutex
	T0 int64
	t  int64
	v  float64
	bw bstream

	vLeading  uint8
	vTrailing uint8
	finished  bool

	tDelta int32

	dataNum int64
}

func New(t0 int64) *Series {
	s := Series{
		T0:       t0,
		vLeading: ^uint8(0),
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

func (s *Series) Push(t int64, v float64) error {
	if t < s.T0 {
		log.Warnf("Data not valid,skip.T0=%d, data time=%d", s.T0, t)
		return errors.New("Error time")
	}
	s.Lock()
	defer s.Unlock()

	if s.t == 0 {
		// first point
		s.t = t
		s.v = v
		s.tDelta = int32(t - s.T0)
		s.bw.writeBits(uint64(s.tDelta), 14)
		s.bw.writeBits(math.Float64bits(v), 64)
		return nil
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

	vDelta := math.Float64bits(v) ^ math.Float64bits(s.v)

	if vDelta == 0 {
		s.bw.writeBit(zero)
	} else {
		s.bw.writeBit(one)
		vLeading := uint8(bits.LeadingZeros64(vDelta))
		vTrailing := uint8(bits.TrailingZeros64(vDelta))

		if vLeading >= 32 {
			vLeading = 31
		}

		if s.vLeading != ^uint8(0) && vLeading >= s.vLeading && vTrailing >= s.vTrailing {
			s.bw.writeBit(zero)
			s.bw.writeBits(vDelta>>s.vTrailing, 64-int(s.vLeading)-int(s.vTrailing))
		} else {
			s.vLeading, s.vTrailing = vLeading, vTrailing

			s.bw.writeBit(one)
			s.bw.writeBits(uint64(vLeading), 5)
			sigbits := 64 - vLeading - vTrailing
			s.bw.writeBits(uint64(sigbits), 6)
			s.bw.writeBits(vDelta>>vTrailing, int(sigbits))
		}
	}

	s.tDelta = tDelta

	s.t = t
	s.v = v

	s.dataNum++

	return nil
}

func (s *Series) Len() int64 {
	return s.dataNum
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
	t  int64
	v  float64

	br        bstream
	vLeading  uint8
	vTrailing uint8

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

		it.v = math.Float64frombits(cnt)
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
		it.v = it.v
	} else {
		bit, itErr := it.br.readBit()
		if itErr != nil {
			it.err = err
			return false
		}
		if bit == zero {
			it.vLeading, it.vTrailing = it.vLeading, it.vTrailing
		} else {
			bits, err := it.br.readBits(5)
			if err != nil {
				it.err = err
				return false
			}

			it.vLeading = uint8(bits)

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
			it.vTrailing = 64 - it.vLeading - mbits
		}

		mbits := int(64 - it.vLeading - it.vTrailing)
		bits, err := it.br.readBits(mbits)
		if err != nil {
			it.err = err
			return false
		}
		vbits := math.Float64bits(it.v)
		vbits ^= (bits << it.vTrailing)
		it.v = math.Float64frombits(vbits)
	}
	return true
}

func (it *Iter) Values() (int64, float64) {
	return it.t, it.v
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
	em.write(s.vLeading)
	em.write(s.t)
	em.write(s.tDelta)
	em.write(s.vTrailing)
	em.write(s.v)
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
	em.read(&s.vLeading)
	em.read(&s.t)
	em.read(&s.tDelta)
	em.read(&s.vTrailing)
	em.read(&s.v)
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
