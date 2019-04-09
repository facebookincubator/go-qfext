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
)

// MaxLoadingFactor specifies the boundary at which we will double
// the quotient filter hash table and also is used to initially size
// the table.
const MaxLoadingFactor = 0.65

// QF is a quotient filter representation
type QF struct {
	entries      uint
	size         uint
	filter       Vector
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
func (qf *QF) DebugDump(full bool) {
	fmt.Printf("\nquotient filter is %d large (%d q bits) with %d entries (loaded %0.3f)\n",
		qf.size, qf.qBits, qf.entries, float64(qf.entries)/float64(qf.size))

	if full {
		fmt.Printf("  bucket  O C S remainder->\n")
		skipped := 0
		for i := uint(0); i < qf.size; i++ {
			o, c, s := 0, 0, 0
			sd := qf.read(i)
			if sd.occupied() {
				o = 1
			}
			if sd.continuation() {
				c = 1
			}
			if sd.shifted() {
				s = 1
			}
			if sd.empty() {
				skipped++
			} else {
				if skipped > 0 {
					fmt.Printf("          ...\n")
					skipped = 0
				}
				r := sd.r()
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
}

// iterate the qf and call the callback once for each hash value present
func (qf *QF) eachHashValue(cb func(uint, uint)) {
	// a stack of q values
	stack := []uint{}
	// let's start from an unshifted value
	start := uint(0)
	for qf.read(start).shifted() {
		qf.right(&start)
	}
	end := start
	qf.left(&end)
	for i := start; true; qf.right(&i) {
		sd := qf.read(i)
		if !sd.continuation() && len(stack) > 0 {
			stack = stack[1:]
		}
		if sd.occupied() {
			stack = append(stack, i)
		}
		if len(stack) > 0 {
			r := sd.r()
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
		QBits:                 MinQBits,
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

	qbits := c.QBits
	if qbits < MinQBits {
		qbits = MinQBits
	}

	qf.initForQuotientBits(qbits)

	qf.filter = c.Representation.RemainderAllocFn(3+BitsPerWord-qbits, qf.size)
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

type slotData uint

const (
	occupiedMask     = slotData(1)
	continuationMask = slotData(1 << 1)
	shiftedMask      = slotData(1 << 2)
	bookkeepingMask  = slotData(0x7)
)

func (sd slotData) empty() bool {
	return (sd & bookkeepingMask) == 0
}

func (sd slotData) occupied() bool {
	return (sd & occupiedMask) != 0
}

func (sd *slotData) setOccupied(on bool) {
	if on {
		*sd |= occupiedMask
	} else {
		*sd &= ^occupiedMask
	}
}

func (sd slotData) continuation() bool {
	return (sd & continuationMask) != 0
}

func (sd *slotData) setContinuation(on bool) {
	if on {
		*sd |= continuationMask
	} else {
		*sd &= ^continuationMask
	}
}

func (sd slotData) shifted() bool {
	return (sd & shiftedMask) != 0
}

func (sd *slotData) setShifted(on bool) {
	if on {
		*sd |= shiftedMask
	} else {
		*sd &= ^shiftedMask
	}
}

func (sd slotData) r() uint {
	return uint(sd >> 3)
}

func (sd *slotData) setR(r uint) {
	*sd = (*sd & bookkeepingMask) | slotData(r<<3)
}

func (qf *QF) read(slot uint) slotData {
	return slotData(qf.filter.Get(slot))
}

func (qf *QF) write(slot uint, sd slotData) {
	qf.filter.Set(slot, uint(sd))
}

func (qf *QF) swap(slot uint, sd slotData) slotData {
	return slotData(qf.filter.Swap(slot, uint(sd)))
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
	sd := qf.read(dq)

	// case 1, the slot is empty
	if sd.empty() {
		qf.entries++
		sd.setOccupied(true)
		sd.setR(dr)
		qf.write(uint(dq), sd)
		if qf.storage != nil {
			qf.storage.Set(dq, value)
		}
		return false
	}

	// if the occupied bit is set for this dq, then we are
	// extending an existing run
	extendingRun := sd.occupied()

	// mark occupied if we are not extending a run
	if !extendingRun {
		sd.setOccupied(true)
		qf.write(dq, sd)
	}

	// ok, let's find the start
	runStart := dq
	if sd.shifted() {
		runStart = qf.findStart(dq)
	}
	// now let's find the spot within the run
	slot := runStart
	if extendingRun {
		sd = qf.read(slot)
		for {
			if sd.empty() || sd.r() >= dr {
				break
			}
			qf.right(&slot)
			sd = qf.read(slot)
			if !sd.continuation() {
				break
			}
		}
	}

	// case 2, the value is already in the filter
	if dr == sd.r() {
		// update value
		if qf.storage != nil {
			qf.storage.Set(slot, value)
		}
		return true
	}
	qf.entries++

	// case 3: we have to insert into an existing run
	// we are writing remainder <dr> into <slot>
	shifted := (slot != uint(dq))
	continuation := slot != runStart

	for {
		// dr -> the remainder to write here
		if qf.storage != nil {
			value = qf.storage.Swap(slot, value)
		}
		var new slotData
		new.setShifted(shifted)
		new.setContinuation(continuation)
		old := qf.read(slot)
		new.setOccupied(old.occupied())
		new.setR(dr)
		qf.write(slot, new)
		if old.empty() {
			break
		}
		if ((slot == runStart) && extendingRun) || old.continuation() {
			continuation = true
		} else {
			continuation = false
		}
		dr = old.r()
		qf.right(&slot)
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
		sd := qf.read(i)
		if !sd.continuation() {
			complete++
		}
		if !sd.shifted() {
			break
		} else if sd.occupied() {
			runs++
		}
	}
	// scan right to find our run
	for runs > complete {
		qf.right(&dq)
		if !qf.read(dq).continuation() {
			complete++
		}
	}
	return dq
}

// Contains returns whether the byte slice is contained
// within the quotient filter
func (qf *QF) Contains(v []byte) bool {
	found, _ := qf.lookupByHash(qf.hash(v))
	return found
}

// ContainsString returns whether the string is contained

// within the quotient filter
func (qf *QF) ContainsString(s string) bool {
	found, _ := qf.lookupByHash(qf.hash(*(*[]byte)(unsafe.Pointer(&s))))
	return found
}

// Lookup searches for key and returns whether it
// exists, and the value stored with it (if any)
func (qf *QF) Lookup(key []byte) (bool, uint) {
	return qf.lookupByHash(qf.hash(key))
}

func (qf *QF) lookupByHash(dq, dr uint) (bool, uint) {
	sd := qf.read(dq)
	if !sd.occupied() {
		return false, 0
	}
	slot := dq
	if sd.shifted() {
		slot = qf.findStart(dq)
		sd = qf.read(slot)
	}
	for {
		if sd.r() == dr {
			value := uint(0)
			if qf.storage != nil {
				value = qf.storage.Get(slot)
			}
			return true, value
		}
		if sd.r() > dr {
			break
		}
		qf.right(&slot)
		sd = qf.read(slot)
		if !sd.continuation() {
			break
		}
	}
	return false, 0
}

// LookupString searches for key and returns whether it
// exists, and the value stored with it (if any)
func (qf *QF) LookupString(key string) (bool, uint) {
	return qf.lookupByHash(qf.hash((*(*[]byte)(unsafe.Pointer(&key)))))
}

func (qf *QF) hash(v []byte) (q, r uint) {
	hv := qf.hashfn(v)
	dq := hv >> qf.rBits
	dr := hv & qf.rMask
	return uint(dq), uint(dr)
}
