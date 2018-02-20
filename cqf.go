package cqf

import (
	"fmt"
	"math/bits"
	"unsafe"

	"github.com/willf/bitset"
)

type Index struct {
	entries      uint
	occupied     *bitset.BitSet
	continuation *bitset.BitSet
	shifted      *bitset.BitSet
	remainders   *packed
	storage      []uint8
	qBits        uint
	rMask        uint64
}

func (index *Index) Entries() (count uint) {
	return index.countEntries()
}

func (index *Index) countEntries() (count uint) {
	for i := uint64(0); uint(i) < index.remainders.size(); i++ {
		if !index.read(i).empty() {
			count++
		}
	}
	return
}

func (index *Index) DebugDump() {
	fmt.Printf("\n  bucket  O C S remainder->\n")
	skipped := 0
	index.remainders.each(func(i uint64, r uint64) {
		o, c, s := 0, 0, 0
		md := index.read(i)
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
			fmt.Printf("%8d  %d %d %d %x\n", i, o, c, s, r)
		}
	})
	if skipped > 0 {
		fmt.Printf("          ...\n")
	}
}

// iterate the index and call the callback once for each hash value present
func (index *Index) eachHashValue(cb func(uint64)) {
	// a stack of q values
	stack := []uint64{}
	index.remainders.each(func(i uint64, r uint64) {
		md := index.read(i)
		if !md.continuation && len(stack) > 0 {
			stack = stack[1:]
		}
		if md.occupied {
			stack = append(stack, i)
		}
		if len(stack) > 0 {
			cb((stack[0] << (64 - index.qBits)) | r)
		}
	})
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

func New(c Config) *Index {
	var ix Index
	n := c.BucketCount()
	ix.occupied = bitset.New(n)
	ix.continuation = bitset.New(n)
	ix.shifted = bitset.New(n)
	ix.remainders = newPacked(uint8(64-c.QBits), n)
	ix.storage = make([]uint8, n)
	ix.qBits = c.QBits
	for i := uint(0); i < (64 - c.QBits); i++ {
		ix.rMask |= 1 << i
	}
	return &ix
}

type metadata struct {
	occupied     bool
	continuation bool
	shifted      bool
}

func (md metadata) empty() bool {
	return !md.occupied && !md.continuation && !md.shifted
}

func (index *Index) read(slot uint64) metadata {
	return metadata{
		occupied:     index.occupied.Test(uint(slot)),
		continuation: index.continuation.Test(uint(slot)),
		shifted:      index.shifted.Test(uint(slot)),
	}
}

func (index *Index) CheckConsistency() error {
	if index.countEntries() != index.entries {
		return fmt.Errorf("%d items added, only %d found", index.entries, index.countEntries())
	}

	// now let's ensure that for every set occupied bit there is a
	// non-zero length run
	usage := map[uint64]uint64{}

	for i := uint64(0); i < uint64(index.remainders.size()); i++ {
		md := index.read(i)
		if !md.occupied {
			continue
		}
		dq := i
		runStart := index.findStart(dq)
		// ok, for bucket dq we've got a run starting at runStart
		for {
			who, used := usage[runStart]
			if used {
				return fmt.Errorf("slot %d used by both dq %d and %d", runStart, dq, who)
			}
			usage[runStart] = dq
			index.right(&runStart)
			md := index.read(runStart)
			if !md.continuation {
				break
			}
		}
	}
	if uint(len(usage)) != index.entries {
		return fmt.Errorf("records show %d entries in index, found %d via scanning",
			index.entries, len(usage))
	}

	return nil
}

func (index *Index) InsertString(s string, count uint64) {
	index.Insert(*(*[]byte)(unsafe.Pointer(&s)), count)
}

func (index *Index) Insert(v []byte, count uint64) {
	dq, dr := index.hash(v)

	md := index.read(dq)

	// if the occupied bit is set for this dq, then we are
	// extending an existing run
	extendingRun := md.occupied

	//  XXX: we can verify not set when it shouldn't be
	index.occupied.Set(uint(dq))

	// easy case!
	if md.empty() {
		index.entries++
		index.remainders.set(dq, dr)
		return
	}

	// ok, let's find the start
	runStart := index.findStart(dq)

	// now let's find the spot within the run
	slot := runStart
	if extendingRun {
		md = index.read(slot)
		for {
			if md.empty() || index.remainders.get(slot) >= dr {
				break
			}
			index.right(&slot)
			md = index.read(slot)
			if !md.continuation {
				break
			}
		}
	}

	if dr == index.remainders.get(slot) {
		// duplicate! XXX: count
		return
	}
	index.entries++

	// we are writing remainder <dr> into <slot>

	// ensure the continuation bit is set correctly
	shifted := slot != dq
	md.continuation = slot > runStart

	for {
		dr = index.remainders.set(slot, dr)
		nxt := index.read(slot)
		if (slot == runStart) && extendingRun {
			nxt.continuation = true
		}
		index.continuation.SetTo(uint(slot), md.continuation)
		index.shifted.SetTo(uint(slot), shifted)
		index.right(&slot)
		md = nxt
		if md.empty() {
			break
		}
		shifted = true
	}
}

func (index *Index) right(i *uint64) {
	*i++
	if *i >= uint64(index.remainders.size()) {
		*i = 0
	}
}

func (index *Index) left(i *uint64) {
	if *i == 0 {
		*i += uint64(index.remainders.size())
	}
	*i--
}

func (index *Index) findStart(dq uint64) uint64 {
	// scan left to figure out how much to skip
	runs, complete := 1, 0
	for i := dq; true; index.left(&i) {
		if !index.continuation.Test(uint(i)) {
			complete++
		}
		if !index.shifted.Test(uint(i)) {
			break
		} else if index.occupied.Test(uint(i)) {
			runs++
		}
	}
	// scan right to find our run
	for runs > complete {
		index.right(&dq)
		if !index.continuation.Test(uint(dq)) {
			complete++
		}
	}
	return dq
}

func (index *Index) Contains(v []byte) bool {
	found, _ := index.Lookup(v)
	return found
}

func (index *Index) ContainsString(s string) bool {
	return index.Contains(*(*[]byte)(unsafe.Pointer(&s)))
}

func (index *Index) Lookup(v []byte) (bool, uint64) {
	dq, dr := index.hash(v)
	if !index.occupied.Test(uint(dq)) {
		return false, 0
	}
	slot := index.findStart(dq)
	for {
		sv := index.remainders.get(slot)
		if sv == dr {
			return true, 0 // XXX count
		}
		if sv > dr {
			return false, 0
		}
		index.right(&slot)
		if !index.continuation.Test(uint(slot)) {
			return false, 0
		}
	}
	return false, 0
}

func (index *Index) LookupString(s string) (bool, uint64) {
	return index.Lookup(*(*[]byte)(unsafe.Pointer(&s)))
}

const (
	offset64 = uint64(14695981039346656037)
	prime64  = uint64(1099511628211)
)

func (index *Index) hash(v []byte) (q, r uint64) {
	// inline fnv 64
	hv := offset64
	for _, c := range v {
		hv *= prime64
		hv ^= uint64(c)
	}
	dq := hv >> (64 - index.qBits)
	dr := hv & index.rMask
	return dq, dr
}
