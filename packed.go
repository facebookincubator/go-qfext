package qf

import (
	"encoding/binary"
	"io"
	"math/bits"
	"reflect"
	"unsafe"

	"fmt"
)

const BitsPerWord = bits.UintSize
const BytesPerWord = BitsPerWord >> 3

type packed struct {
	forbiddenMask uint
	bits          uint
	space         []uint
	size          uint
}

var _ Vector = (*packed)(nil)

func BitPackedVectorAllocate(bits uint, size uint) Vector {
	if bits > BitsPerWord {
		panic(fmt.Sprintf("bit size of %d is greater than word size of %s, not supported",
			bits, BitsPerWord))
	}

	// calculate required space.
	words := wordsRequired(bits, size)
	return &packed{genForbiddenMask(bits), bits, make([]uint, words), size}
}

func wordsRequired(bits, count uint) (words uint) {
	words = ((count * bits) / BitsPerWord) + 1
	return
}

func genForbiddenMask(bits uint) uint {
	return ^((1 << bits) - 1)
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

const PackedVectorVersion = uint64(0x100000001)

func (v packed) WriteTo(stream io.Writer) (n int64, err error) {
	if err = binary.Write(stream, binary.LittleEndian, PackedVectorVersion); err != nil {
		return
	}
	n += int64(unsafe.Sizeof(PackedVectorVersion))
	if err = binary.Write(stream, binary.LittleEndian, uint64(v.bits)); err != nil {
		return
	}
	n += int64(unsafe.Sizeof(uint64(v.bits)))
	if err = binary.Write(stream, binary.LittleEndian, uint64(v.size)); err != nil {
		return
	}
	n += int64(unsafe.Sizeof(uint64(v.size)))

	// now directly copy the bytes backing the packed data representation, because
	// FAST

	// Get the slice header
	header := *(*reflect.SliceHeader)(unsafe.Pointer(&v.space))

	// The length and capacity of the slice are different.
	header.Len *= BytesPerWord
	header.Cap *= BytesPerWord

	// Convert slice header to an []byte
	data := *(*[]byte)(unsafe.Pointer(&header))
	if wrote, e := stream.Write(data); e != nil {
		err = e
	} else {
		expected := len(v.space) * BytesPerWord
		if wrote != expected {
			err = fmt.Errorf("wrote %d out of expected %d", wrote, expected)
		} else {
			n += int64(wrote)
		}
	}

	return
}

func (v *packed) ReadFrom(stream io.Reader) (n int64, err error) {
	var ver, bits, count uint64
	if err = binary.Read(stream, binary.LittleEndian, &ver); err != nil {
		return
	}
	n += int64(unsafe.Sizeof(ver))
	if err = binary.Read(stream, binary.LittleEndian, &bits); err != nil {
		return
	}
	n += int64(unsafe.Sizeof(bits))
	if err = binary.Read(stream, binary.LittleEndian, &count); err != nil {
		return
	}
	n += int64(unsafe.Sizeof(count))
	words := wordsRequired(uint(bits), uint(count))
	raw := make([]byte, words*BytesPerWord)
	if rd, e := stream.Read(raw); e != nil {
		err = e
	} else {
		n += int64(rd)
		expected := words * BytesPerWord
		if rd != int(expected) {
			err = fmt.Errorf("short read.  wanted %d got %d", expected, rd)
		} else {
			header := *(*reflect.SliceHeader)(unsafe.Pointer(&raw))
			header.Len /= BytesPerWord
			header.Cap /= BytesPerWord
			v.space = *(*[]uint)(unsafe.Pointer(&header))
			v.bits = uint(bits)
			v.size = uint(count)
			v.forbiddenMask = genForbiddenMask(uint(bits))
		}
	}
	return
}
