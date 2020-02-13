// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved

// Package qf implements a quotient filter data
// structure which supports:
//  1. external storage per entry
//  2. dynamic doubling
//  3. packed or unpacked representations (choose time or space)
//  4. a user overrideable hash function (default is murmur)
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

// Filter is a quotient filter representation
type Filter struct {
	entries      uint64
	size         uint64
	filter       Vector
	storage      Vector
	rBits, qBits uint
	rMask        uint64
	maxEntries   uint64
	config       Config
	hashfn       HashFn
	allocfn      VectorAllocateFn
}

// Len returns the number of entries in the quotient filter
func (qf *Filter) Len() uint64 {
	return qf.entries
}

// DebugDump prints a textual representation of the quotient filter
// to stdout
func (qf *Filter) DebugDump(full bool) {
	fmt.Printf("\nquotient filter is %d large (%d q bits) with %d entries (loaded %0.3f)\n",
		qf.size, qf.qBits, qf.entries, float64(qf.entries)/float64(qf.size))

	if full {
		fmt.Printf("  bucket  O C S remainder->\n")
		skipped := 0
		for i := uint64(0); i < uint64(qf.size); i++ {
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
				v := uint64(0)
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
func (qf *Filter) eachHashValue(cb func(uint64, uint64)) {
	// a stack of q values
	stack := []uint64{}
	// let's start from an unshifted value
	start := uint64(0)
	for qf.read(start).shifted() {
		right(&start, qf.size)
	}
	end := start
	left(&end, qf.size)
	for i := start; true; right(&i, qf.size) {
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
func New() *Filter {
	return NewWithConfig(Config{})
}

// NewWithConfig allocates a new quotient filter based on the
// supplied configuration
func NewWithConfig(c Config) *Filter {
	var qf Filter
	if c.BitPacked {
		qf.allocfn = BitPackedVectorAllocate
	} else {
		qf.allocfn = UnpackedVectorAllocate
	}
	if c.HashFn == nil {
		c.HashFn = murmurhash64
	}
	qf.hashfn = c.HashFn

	qbits := c.QBits()

	qf.initForQuotientBits(uint(qbits))

	qf.config = c

	qf.allocStorage()

	if qf.maxEntries > qf.size {
		panic("internal inconsistency")
	}
	return &qf
}

// BitsOfStoragePerEntry reports the configured external storage for the
// quotient filter
func (qf *Filter) BitsOfStoragePerEntry() uint {
	return qf.config.BitsOfStoragePerEntry
}

func (qf *Filter) allocStorage() {
	qf.filter = qf.allocfn(3+bitsPerWord-qf.qBits, qf.size)
	if qf.config.BitsOfStoragePerEntry > 0 {
		qf.storage = qf.allocfn(qf.config.BitsOfStoragePerEntry, qf.size)
	}
}

func (qf *Filter) initForQuotientBits(qBits uint) {
	qf.qBits = qBits
	qf.rBits, qf.rMask, qf.size = initForQuotientBits(qBits)
	qf.rBits = (bitsPerWord - qBits)
	qf.rMask = 0
	for i := uint(0); i < qf.rBits; i++ {
		qf.rMask |= 1 << i
	}
	qf.maxEntries = uint64(math.Ceil(float64(qf.size) * MaxLoadingFactor))
}

func initForQuotientBits(qBits uint) (rBits uint, rMask, size uint64) {
	size = 1 << (uint64(qBits))
	rBits = (bitsPerWord - qBits)
	for i := uint(0); i < rBits; i++ {
		rMask |= 1 << i
	}
	return
}

type slotData uint64

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

func (sd slotData) r() uint64 {
	return uint64(sd >> 3)
}

func (sd *slotData) setR(r uint64) {
	*sd = (*sd & bookkeepingMask) | slotData(r<<3)
}

func (qf *Filter) read(slot uint64) slotData {
	return slotData(qf.filter.Get(slot))
}

func (qf *Filter) write(slot uint64, sd slotData) {
	qf.filter.Set(slot, uint64(sd))
}

func (qf *Filter) swap(slot uint64, sd slotData) slotData {
	return slotData(qf.filter.Swap(slot, uint64(sd)))
}

func (qf *Filter) countEntries() (count uint64) {
	for i := uint64(0); i < qf.size; i++ {
		if !qf.read(i).empty() {
			count++
		}
	}
	return
}

// InsertStringWithValue stores the string key and an associated
// integer value in the quotient filter it returns whether the
// key was already present in the quotient filter.
func (qf *Filter) InsertStringWithValue(s string, value uint64) bool {
	return qf.InsertWithValue(*(*[]byte)(unsafe.Pointer(&s)), value)
}

// InsertString stores the string key in the quotient filter and
// returns whether this string was already present
func (qf *Filter) InsertString(s string) bool {
	return qf.InsertStringWithValue(s, 0)
}

func (qf *Filter) double() {
	// start with a shallow coppy
	cpy := *qf
	cpy.entries = 0
	cpy.initForQuotientBits(cpy.qBits + 1)
	cpy.allocStorage()
	qf.eachHashValue(func(hv uint64, slot uint64) {
		dq := hv >> cpy.rBits
		dr := hv & cpy.rMask
		var v uint64
		if qf.storage != nil {
			v = qf.storage.Get(slot)
		}
		cpy.insertByHash(dq, dr, v)
	})

	// shallow copy back over self
	*qf = cpy
}

// InsertWithValue stores the key (byte slice) and an integer value in
// the quotient filter.  It returns whether a value already existed.
func (qf *Filter) InsertWithValue(v []byte, value uint64) (update bool) {
	if qf.maxEntries <= qf.entries {
		qf.double()
	}
	dq, dr := hash(qf.hashfn, v, qf.rBits, qf.rMask)
	return qf.insertByHash(uint64(dq), uint64(dr), value)
}

// Insert stores the key (byte slice) in the quotient filter it
// returns whether it already existed
func (qf *Filter) Insert(v []byte) (update bool) {
	return qf.InsertWithValue(v, 0)
}

func (qf *Filter) insertByHash(dq, dr, value uint64) bool {
	sd := qf.read(dq)

	// case 1, the slot is empty
	if sd.empty() {
		qf.entries++
		sd.setOccupied(true)
		sd.setR(dr)
		qf.write(uint64(dq), sd)
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
		runStart = findStart(dq, qf.size, qf.filter.Get)
	}
	// now let's find the spot within the run
	slot := runStart
	if extendingRun {
		sd = qf.read(slot)
		for {
			if sd.empty() || sd.r() >= dr {
				break
			}
			right(&slot, qf.size)
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
	shifted := (slot != uint64(dq))
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
		right(&slot, qf.size)
		shifted = true
	}
	return false
}

func right(i *uint64, size uint64) {
	*i++
	if *i >= size {
		*i = 0
	}
}

func left(i *uint64, size uint64) {
	if *i == 0 {
		*i += size
	}
	*i--
}

// XXX: error
func findStart(dq uint64, size uint64, read readFn) uint64 {
	// scan left to figure out how much to skip
	runs, complete := 1, 0
	for i := dq; true; left(&i, size) {
		sd := slotData(read(i))
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
		right(&dq, size)
		if !slotData(read(dq)).continuation() {
			complete++
		}
	}
	return dq
}

// Contains returns whether the byte slice is contained
// within the quotient filter
func (qf *Filter) Contains(v []byte) bool {
	found, _ := qf.Lookup(v)
	return found
}

// ContainsString returns whether the string is contained

// within the quotient filter
func (qf *Filter) ContainsString(s string) bool {
	found, _ := qf.Lookup(*(*[]byte)(unsafe.Pointer(&s)))
	return found
}

// Lookup searches for key and returns whether it
// exists, and the value stored with it (if any)
func (qf *Filter) Lookup(key []byte) (bool, uint64) {
	dq, dr := hash(qf.hashfn, key, qf.rBits, qf.rMask)
	var storageFn readFn
	if qf.storage != nil {
		storageFn = qf.storage.Get
	}
	return lookupByHash(dq, dr, qf.size, qf.filter.Get, storageFn)
}

func lookupByHash(dq, dr, size uint64, read, storage readFn) (bool, uint64) {
	sd := slotData(read(dq))
	if !sd.occupied() {
		return false, 0
	}
	slot := dq
	if sd.shifted() {
		slot = findStart(dq, size, read)
		sd = slotData(read(slot))
	}
	for {
		if sd.r() == dr {
			value := uint64(0)
			if storage != nil {
				value = storage(slot)
			}
			return true, value
		}
		if sd.r() > dr {
			break
		}
		right(&slot, size)
		sd = slotData(read(slot))
		if !sd.continuation() {
			break
		}
	}
	return false, 0
}

// LookupString searches for key and returns whether it
// exists, and the value stored with it (if any)
func (qf *Filter) LookupString(key string) (bool, uint64) {
	return qf.Lookup(*(*[]byte)(unsafe.Pointer(&key)))
}

func hash(fn HashFn, v []byte, rBits uint, rMask uint64) (q, r uint64) {
	hv := fn(v)
	dq := hv >> rBits
	dr := hv & rMask
	return uint64(dq), uint64(dr)
}
