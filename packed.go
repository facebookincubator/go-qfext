package qf

import (
	"io"
	"math/bits"

	"fmt"
)

const WordSize = bits.UintSize

type packed struct {
	forbiddenMask uint
	bits          uint
	space         []uint
	size          uint
}

var _ Vector = (*packed)(nil)

func BitPackedVectorAllocate(bits uint, size uint) Vector {
	if bits > WordSize {
		panic(fmt.Sprintf("bit size of %d is greater than word size of %s, not supported",
			bits, WordSize))
	}
	var forbiddenMask uint
	bit := uint(1)
	for i := 0; i < int(bits); i++ {
		forbiddenMask |= bit
		bit <<= 1
	}
	forbiddenMask = ^forbiddenMask

	// calculate required space.
	words := ((size * bits) / WordSize) + 1
	return &packed{forbiddenMask, bits, make([]uint, words), size}
}

func (p *packed) Swap(ix uint, val uint) (oldval uint) {
	// XXX this could be more efficient
	oldval = p.Get(ix)
	p.Set(ix, val)
	return
}

func (p *packed) Set(ix uint, val uint) {
	if val&p.forbiddenMask != 0 {
		panic(fmt.Sprintf("attempt to store out of range value.  numeric overflow: %x (%x)", (val & p.forbiddenMask), val))
	}
	bitstart := ix * uint(p.bits)
	word := bitstart / 64
	bitoff := bitstart % 64
	getbits := 64 - (bitoff)
	if getbits > uint(p.bits) {
		getbits = uint(p.bits)
	}
	// zero
	p.space[word] =
		((p.space[word] >> (bitoff + getbits)) << (bitoff + getbits)) |
			(p.space[word] << (64 - bitoff) >> (64 - bitoff))

	// or in val
	p.space[word] |= (val << bitoff)

	if getbits < uint(p.bits) {
		remainder := uint(p.bits) - getbits
		p.space[word+1] = ((p.space[word+1] >> remainder) << remainder) | val>>getbits
	}
	return
}

func (p *packed) Get(ix uint) (val uint) {
	bitstart := ix * uint(p.bits)
	word := bitstart / 64
	bitoff := bitstart % 64
	getbits := 64 - (bitoff)
	if getbits > uint(p.bits) {
		getbits = uint(p.bits)
	}
	// now get 'getbits' from 'word' starting at 'bitoff'
	sl := (64 - getbits - bitoff)
	val = (p.space[word] << sl)
	sr := (64 - getbits)
	val >>= sr
	if getbits < uint(p.bits) {
		remainder := uint(p.bits) - getbits
		x := (p.space[word+1] << (64 - remainder)) >> (64 - remainder)
		val |= x << getbits
	}
	return val
}

func (v packed) WriteTo(w io.Writer) (n int64, err error) {
	return 0, fmt.Errorf("not implemented")
}

func (v packed) Read(r io.Reader) (n Vector, err error) {
	return nil, fmt.Errorf("not implemented")
}
