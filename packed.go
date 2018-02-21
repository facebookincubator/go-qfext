package qf

import (
	"fmt"
)

type packed struct {
	forbiddenMask uint64
	bits          uint8
	space         []uint64
	size          uint
}

func newPacked(bits uint8, size uint) *packed {
	var forbiddenMask uint64
	bit := uint64(1)
	for i := 0; i < int(bits); i++ {
		forbiddenMask |= bit
		bit <<= 1
	}
	forbiddenMask = ^forbiddenMask

	// calculate required space.
	words := (size * uint(bits) / 64) + 1
	return &packed{forbiddenMask, bits, make([]uint64, words), size}
}

//                 | bitoff, the bit offset into the word
//                 V
//                   1 1 1 1 1 1 1 1 1 1 2 2 2 2 2 2 2 2 2 2 3 3 3
// 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2
//                 \---------------/
//                    getbits - the number of interesting bits in this
//                              word
//
func (p *packed) set(ix uint, val uint64) (oldval uint64) {
	if val&p.forbiddenMask != 0 {
		panic(fmt.Sprintf("attempt to store out of range value.  numeric overflow: %x (%x)", (val & p.forbiddenMask), val))
	}
	// XXX this should be more efficient
	oldval = p.get(ix)
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

func (p *packed) get(ix uint) (val uint64) {
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

func (p *packed) len() uint {
	return p.size
}
