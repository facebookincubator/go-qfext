package qf

import (
	"fmt"
	"math"
	"math/bits"
	"unsafe"

	"github.com/willf/bitset"
)

type QF struct {
	entries    uint
	metadata   *bitset.BitSet
	remainders *packed
	storage    []uint8
	qBits      uint
	rMask      uint64
	maxEntries uint
}

func (qf *QF) Entries() (count uint) {
	return qf.countEntries()
}

func (qf *QF) countEntries() (count uint) {
	for i := uint(0); i < qf.remainders.len(); i++ {
		if !qf.read(i).empty() {
			count++
		}
	}
	return
}

func (qf *QF) DebugDump() {
	fmt.Printf("\n  bucket  O C S remainder->\n")
	skipped := 0
	for i := uint(0); i < qf.remainders.len(); i++ {
		o, c, s := 0, 0, 0
		md := qf.read(i)
		if md.occupied {
			o = 1
		}
		if md.continuation {
			c = 1
		}
		if md.shifted {
			s = 1
		}
		if md.empty() {
			skipped++
		} else {
			if skipped > 0 {
				fmt.Printf("          ...\n")
				skipped = 0
			}
			r := qf.remainders.get(i)
			fmt.Printf("%8d  %d %d %d %x\n", i, o, c, s, r)
		}
	}
	if skipped > 0 {
		fmt.Printf("          ...\n")
	}
}

// iterate the qf and call the callback once for each hash value present
func (qf *QF) eachHashValue(cb func(uint64)) {
	// a stack of q values
	stack := []uint64{}
	// let's start from an unshifted value
	start := uint(0)
	for qf.read(start).shifted {
		qf.right(&start)
	}
	end := start
	qf.left(&end)
	for i := start; true; qf.right(&i) {
		md := qf.read(i)
		if !md.continuation && len(stack) > 0 {
			stack = stack[1:]
		}
		if md.occupied {
			stack = append(stack, uint64(i))
		}
		if len(stack) > 0 {
			r := qf.remainders.get(i)
			cb((stack[0] << (64 - qf.qBits)) | (r & qf.rMask))
		}
		if i == end {
			break
		}
	}
}

func DetermineSize(numberOfEntries uint64, bitsOfStoragePerEntry uint8) Config {
	x := uint64(1)
	for x < (numberOfEntries * 2) {
		x <<= 1
	}
	return Config{
		ExpectedNumberOfEntries: numberOfEntries,
		QBits: uint(bits.TrailingZeros64(x)),
		BitsOfStoragePerEntry: bitsOfStoragePerEntry,
	}
}

type Config struct {
	ExpectedNumberOfEntries uint64
	QBits                   uint
	BitsOfStoragePerEntry   uint8
}

func (c *Config) ExpectedLoading() float64 {
	return 100. * float64(c.ExpectedNumberOfEntries) / float64(c.BucketCount())
}

func (c *Config) BytesRequired() uint {
	bitsPerEntry := uint(64) + 3 + 8
	return c.BucketCount() * bitsPerEntry / 8
}

func (c *Config) BucketCount() uint {
	return 1 << (uint(c.QBits))
}

func (c *Config) Explain() {
	fmt.Printf("%d bits needed for quotient (%d buckets)\n", c.QBits, c.BucketCount())
	fmt.Printf("%0.2f%% loading expected\n", c.ExpectedLoading())
	fmt.Printf("%d bytes required\n", c.BytesRequired())
}

const DefaultQFBits = 4

func New() *QF {
	return NewWithConfig(Config{
		QBits: DefaultQFBits,
		BitsOfStoragePerEntry: 0, // XXX
	})
}

func NewWithConfig(c Config) *QF {
	var qf QF
	n := c.BucketCount()
	qf.metadata = bitset.New(n * 3)
	qf.remainders = newPacked(uint8(64-c.QBits), n)
	qf.storage = make([]uint8, n)
	qf.qBits = c.QBits
	for i := uint(0); i < (64 - c.QBits); i++ {
		qf.rMask |= 1 << i
	}
	qf.maxEntries = uint(math.Ceil(float64(n) * 0.65))
	if qf.maxEntries > n {
		panic("internal inconsistency")
	}
	return &qf
}

type metadata struct {
	occupied     bool
	continuation bool
	shifted      bool
}

func (md metadata) empty() bool {
	return !md.occupied && !md.continuation && !md.shifted
}

func (qf *QF) read(slot uint) metadata {
	return metadata{
		occupied:     qf.metadata.Test(slot * 3),
		continuation: qf.metadata.Test(slot*3 + 1),
		shifted:      qf.metadata.Test(slot*3 + 2),
	}
}

func (qf *QF) occupied(slot uint) bool {
	return qf.metadata.Test(slot * 3)
}

func (qf *QF) setOccupied(slot uint) {
	qf.metadata.Set(slot * 3)
}

func (qf *QF) continuation(slot uint) bool {
	return qf.metadata.Test(slot*3 + 1)
}

func (qf *QF) setContinuation(slot uint) {
	qf.metadata.Set(slot*3 + 1)
}

func (qf *QF) setContinuationTo(slot uint, to bool) {
	qf.metadata.SetTo(slot*3+1, to)
}

func (qf *QF) shifted(slot uint) bool {
	return qf.metadata.Test(slot*3 + 2)
}

func (qf *QF) setShifted(slot uint) {
	qf.metadata.Set(slot*3 + 2)
}

func (qf *QF) setShiftedTo(slot uint, to bool) {
	qf.metadata.SetTo(slot*3+2, to)
}

func (qf *QF) CheckConsistency() error {
	if qf.countEntries() != qf.entries {
		return fmt.Errorf("%d items added, only %d found", qf.entries, qf.countEntries())
	}

	// now let's ensure that for every set occupied bit there is a
	// non-zero length run
	usage := map[uint]uint{}

	for i := uint(0); i < qf.remainders.len(); i++ {
		md := qf.read(i)
		if !md.occupied {
			continue
		}
		dq := i
		runStart := qf.findStart(dq)
		// ok, for bucket dq we've got a run starting at runStart
		for {
			who, used := usage[runStart]
			if used {
				return fmt.Errorf("slot %d used by both dq %d and %d", runStart, dq, who)
			}
			usage[runStart] = dq
			qf.right(&runStart)
			md := qf.read(runStart)
			if !md.continuation {
				break
			}
		}
	}
	if uint(len(usage)) != qf.entries {
		return fmt.Errorf("records show %d entries in qf, found %d via scanning",
			qf.entries, len(usage))
	}

	return nil
}

func (qf *QF) InsertString(s string, count uint64) {
	qf.Insert(*(*[]byte)(unsafe.Pointer(&s)), count)
}

func (qf *QF) double() {
	cpy := NewWithConfig(Config{
		QBits: qf.qBits + 1,
		BitsOfStoragePerEntry: 0, // XXX
	})
	qf.eachHashValue(func(hv uint64) {
		dq := hv >> (64 - cpy.qBits)
		dr := hv & cpy.rMask
		cpy.insertByHash(dq, dr, 0) // XXX count
	})

	// shallow copy in
	*qf = *cpy
}

func (qf *QF) Insert(v []byte, count uint64) {
	if qf.maxEntries <= qf.entries {
		qf.double()
	}
	dq, dr := qf.hash(v)
	qf.insertByHash(dq, dr, count)

	if e := qf.CheckConsistency(); e != nil {
		qf.DebugDump()
		panic(e.Error())
	}
}

func (qf *QF) insertByHash(dq, dr, count uint64) {
	md := qf.read(uint(dq))

	// if the occupied bit is set for this dq, then we are
	// extending an existing run
	extendingRun := md.occupied

	//  XXX: we can verify not set when it shouldn't be
	qf.setOccupied(uint(dq))

	// easy case!
	if md.empty() {
		qf.entries++
		qf.remainders.set(uint(dq), dr)
		return
	}

	// ok, let's find the start
	runStart := qf.findStart(uint(dq))

	// now let's find the spot within the run
	slot := runStart
	if extendingRun {
		md = qf.read(slot)
		for {
			if md.empty() || qf.remainders.get(slot) >= dr {
				break
			}
			qf.right(&slot)
			md = qf.read(slot)
			if !md.continuation {
				break
			}
		}
	}

	if dr == qf.remainders.get(slot) {
		// duplicate! XXX: count
		return
	}
	qf.entries++

	// we are writing remainder <dr> into <slot>

	// ensure the continuation bit is set correctly
	shifted := (slot != uint(dq))
	md.continuation = slot != runStart

	for {
		dr = qf.remainders.set(slot, dr)
		nxt := qf.read(slot)
		if (slot == runStart) && extendingRun {
			nxt.continuation = true
		}
		qf.setContinuationTo(uint(slot), md.continuation)
		qf.setShiftedTo(uint(slot), shifted)
		qf.right(&slot)
		md = nxt
		if md.empty() {
			break
		}
		shifted = true
	}
}

func (qf *QF) right(i *uint) {
	*i++
	if *i >= qf.remainders.len() {
		*i = 0
	}
}

func (qf *QF) left(i *uint) {
	if *i == 0 {
		*i += qf.remainders.len()
	}
	*i--
}

func (qf *QF) findStart(dq uint) uint {
	// scan left to figure out how much to skip
	runs, complete := 1, 0
	for i := dq; true; qf.left(&i) {
		if !qf.continuation(uint(i)) {
			complete++
		}
		if !qf.shifted(i) {
			break
		} else if qf.occupied(i) {
			runs++
		}
	}
	// scan right to find our run
	for runs > complete {
		qf.right(&dq)
		if !qf.continuation(dq) {
			complete++
		}
	}
	return dq
}

func (qf *QF) Contains(v []byte) bool {
	found, _ := qf.Lookup(v)
	return found
}

func (qf *QF) ContainsString(s string) bool {
	return qf.Contains(*(*[]byte)(unsafe.Pointer(&s)))
}

func (qf *QF) Lookup(v []byte) (bool, uint64) {
	return qf.lookupByHash(qf.hash(v))
}

func (qf *QF) lookupByHash(dq, dr uint64) (bool, uint64) {
	if !qf.occupied(uint(dq)) {
		return false, 0
	}
	slot := qf.findStart(uint(dq))
	for {
		sv := qf.remainders.get(slot)
		if sv == dr {
			return true, 0 // XXX count
		}
		if sv > dr {
			return false, 0
		}
		qf.right(&slot)
		if !qf.continuation(slot) {
			return false, 0
		}
	}
	return false, 0
}

func (qf *QF) LookupString(s string) (bool, uint64) {
	return qf.Lookup(*(*[]byte)(unsafe.Pointer(&s)))
}

const (
	offset64 = uint64(14695981039346656037)
	prime64  = uint64(1099511628211)
)

func (qf *QF) hash(v []byte) (q, r uint64) {
	// inline fnv 64
	hv := offset64
	for _, c := range v {
		hv *= prime64
		hv ^= uint64(c)
	}
	dq := hv >> (64 - qf.qBits)
	dr := hv & qf.rMask
	return dq, dr
}
