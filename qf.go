// Package qf implements a quotient filter data
// structure which supports:
//  1. external storage per entry
//  2. dynamic doubling
//  3. packed or unpacked representations (choose time or space)
//  4. variable hash function
package qf

import (
	"fmt"
	"math"
	"unsafe"

	"github.com/willf/bitset"
)

// MaxLoadingFactor specifies the boundary at which we will double
// the quotient filter hash table and also is used to initially size
// the table.
const MaxLoadingFactor = 0.65

// QF is a quotient filter representation
type QF struct {
	entries      uint
	size         uint
	metadata     *bitset.BitSet
	remainders   Vector
	storage      Vector
	rBits, qBits uint
	rMask        uint
	maxEntries   uint
	config       Config
	hashfn       HashFn
}

// Len returns the number of entries in the quotient filter
func (qf *QF) Len() uint {
	return qf.entries
}

// DebugDump prints a textual representation of the quotient filter
// to stdout
func (qf *QF) DebugDump() {
	fmt.Printf("\n  bucket  O C S remainder->\n")
	skipped := 0
	for i := uint(0); i < qf.size; i++ {
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
			r := qf.remainders.Get(i)
			v := uint(0)
			if qf.storage != nil {
				v = qf.storage.Get(i)
			}
			fmt.Printf("%8d  %d %d %d %x (%d)\n", i, o, c, s, r, v)
		}
	}
	if skipped > 0 {
		fmt.Printf("          ...\n")
	}
}

// iterate the qf and call the callback once for each hash value present
func (qf *QF) eachHashValue(cb func(uint, uint)) {
	// a stack of q values
	stack := []uint{}
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
			stack = append(stack, i)
		}
		if len(stack) > 0 {
			r := qf.remainders.Get(i)
			cb((stack[0]<<qf.rBits)|(r&qf.rMask), i)
		}
		if i == end {
			break
		}
	}
}

// New allocates a new quotient filter with default initial
// sizing and no external storage configured.
func New() *QF {
	return NewWithConfig(Config{
		QBits:                 DefaultQBits,
		BitsOfStoragePerEntry: 0,
	})
}

// NewWithConfig allocates a new quotient filter based on the
// supplied configuration
func NewWithConfig(c Config) *QF {
	var qf QF
	if c.Representation.RemainderAllocFn == nil {
		c.Representation.RemainderAllocFn = DefaultRepresentationConfig.RemainderAllocFn
	}
	if c.Representation.StorageAllocFn == nil {
		c.Representation.StorageAllocFn = DefaultRepresentationConfig.StorageAllocFn
	}
	if c.Representation.HashFn == nil {
		c.Representation.HashFn = DefaultRepresentationConfig.HashFn
	}
	qf.hashfn = c.Representation.HashFn

	qf.initForQuotientBits(c.QBits)

	qf.metadata = bitset.New(qf.size * 3)
	qf.remainders = c.Representation.RemainderAllocFn(BitsPerWord-c.QBits, qf.size)
	if c.BitsOfStoragePerEntry > 0 {
		qf.storage = c.Representation.StorageAllocFn(c.BitsOfStoragePerEntry, qf.size)
	}
	if qf.maxEntries > qf.size {
		panic("internal inconsistency")
	}
	qf.config = c
	return &qf
}

// BitsOfStoragePerEntry reports the configured external storage for the
// quotient filter
func (qf *QF) BitsOfStoragePerEntry() uint {
	return qf.config.BitsOfStoragePerEntry
}

func (qf *QF) initForQuotientBits(qBits uint) {
	qf.qBits = qBits
	qf.size = 1 << (uint(qBits))
	qf.rBits = (BitsPerWord - qBits)
	qf.rMask = 0
	for i := uint(0); i < qf.rBits; i++ {
		qf.rMask |= 1 << i
	}
	qf.maxEntries = uint(math.Ceil(float64(qf.size) * MaxLoadingFactor))
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

func (qf *QF) countEntries() (count uint) {
	for i := uint(0); i < qf.size; i++ {
		if !qf.read(i).empty() {
			count++
		}
	}
	return
}

// InsertStringWithValue stores the string key and an associated
// integer value in the quotient filter it returns whether the
// key was already present in the quotient filter.
func (qf *QF) InsertStringWithValue(s string, value uint) bool {
	return qf.InsertWithValue(*(*[]byte)(unsafe.Pointer(&s)), value)
}

// InsertString stores the string key in the quotient filter and
// returns whether this string was already present
func (qf *QF) InsertString(s string) bool {
	return qf.InsertStringWithValue(s, 0)
}

func (qf *QF) double() {
	cfg := qf.config
	cfg.QBits++
	cpy := NewWithConfig(cfg)
	qf.eachHashValue(func(hv uint, slot uint) {
		dq := hv >> cpy.rBits
		dr := hv & cpy.rMask
		var v uint
		if qf.storage != nil {
			v = qf.storage.Get(slot)
		}
		cpy.insertByHash(dq, dr, v)
	})

	// shallow copy in
	*qf = *cpy
}

// InsertWithValue stores the key (byte slice) and an integer value in
// the quotient filter.  It returns whether a value already existed.
func (qf *QF) InsertWithValue(v []byte, value uint) (update bool) {
	if qf.maxEntries <= qf.entries {
		qf.double()
	}
	dq, dr := qf.hash(v)
	return qf.insertByHash(uint(dq), uint(dr), value)
}

// Insert stores the key (byte slice) in the quotient filter it
// returns whether it already existed
func (qf *QF) Insert(v []byte) (update bool) {
	return qf.InsertWithValue(v, 0)
}

func (qf *QF) insertByHash(dq, dr, value uint) bool {
	md := qf.read(uint(dq))

	// if the occupied bit is set for this dq, then we are
	// extending an existing run
	extendingRun := md.occupied

	qf.setOccupied(uint(dq))

	// easy case!
	if md.empty() {
		qf.entries++
		qf.remainders.Swap(uint(dq), dr)
		if qf.storage != nil {
			qf.storage.Swap(uint(dq), value)
		}
		return false
	}

	// ok, let's find the start
	runStart := qf.findStart(uint(dq))

	// now let's find the spot within the run
	slot := runStart
	if extendingRun {
		md = qf.read(slot)
		for {
			if md.empty() || qf.remainders.Get(slot) >= dr {
				break
			}
			qf.right(&slot)
			md = qf.read(slot)
			if !md.continuation {
				break
			}
		}
	}

	if dr == qf.remainders.Get(slot) {
		// update value
		if qf.storage != nil {
			qf.storage.Swap(slot, value)
		}
		return true
	}
	qf.entries++

	// we are writing remainder <dr> into <slot>

	// ensure the continuation bit is set correctly
	shifted := (slot != uint(dq))
	md.continuation = slot != runStart

	for {
		dr = qf.remainders.Swap(slot, dr)
		if qf.storage != nil {
			value = qf.storage.Swap(slot, value)
		}
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
	return false
}

func (qf *QF) right(i *uint) {
	*i++
	if *i >= qf.size {
		*i = 0
	}
}

func (qf *QF) left(i *uint) {
	if *i == 0 {
		*i += qf.size
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

// Contains returns whether the byte slice is contained
// within the quotient filter
func (qf *QF) Contains(v []byte) bool {
	found, _ := qf.Lookup(v)
	return found
}

// ContainsString returns whether the string is contained
// within the quotient filter
func (qf *QF) ContainsString(s string) bool {
	return qf.Contains(*(*[]byte)(unsafe.Pointer(&s)))
}

// Lookup searches for key and returns whether it
// exists, and the value stored with it (if any)
func (qf *QF) Lookup(key []byte) (bool, uint) {
	return qf.lookupByHash(qf.hash(key))
}

func (qf *QF) lookupByHash(dq, dr uint) (bool, uint) {
	if !qf.occupied(uint(dq)) {
		return false, 0
	}
	slot := qf.findStart(uint(dq))
	for {
		sv := qf.remainders.Get(slot)
		if sv == dr {
			value := uint(0)
			if qf.storage != nil {
				value = qf.storage.Get(slot)
			}
			return true, value
		}
		if sv > dr {
			break
		}
		qf.right(&slot)
		if !qf.continuation(slot) {
			break
		}
	}
	return false, 0
}

// LookupString searches for key and returns whether it
// exists, and the value stored with it (if any)
func (qf *QF) LookupString(key string) (bool, uint) {
	return qf.Lookup(*(*[]byte)(unsafe.Pointer(&key)))
}

func (qf *QF) hash(v []byte) (q, r uint) {
	hv := qf.hashfn(v)
	dq := hv >> qf.rBits
	dr := hv & qf.rMask
	return uint(dq), uint(dr)
}
