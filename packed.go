// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved

package qf

import (
	"encoding/binary"
	"io"
	"os"
	"unsafe"

	"fmt"
)

// qfBitPackedVectorVersion is the version of the packed vector
// serialization format.
const qfBitPackedVectorVersion = uint64(8)

// bitsPerWord is the number of bits in a 64 bit word
const bitsPerWord = 8 * 8

// bytesPerWord is the number of bytes in a 64 bit word
const bytesPerWord = bitsPerWord >> 3

type packedHeader struct {
	Version uint64
	Bits    uint64
	Size    uint64
}

type packed struct {
	forbiddenMask uint64
	bits          uint
	space         []uint64
	size          uint64
}

var _ Vector = (*packed)(nil)

// BitPackedVectorAllocate allocates bitpacked storage with a non-portable
// serialization format (i.e. between architectures)
func BitPackedVectorAllocate(bits uint, size uint64) Vector {
	if bits > bitsPerWord {
		panic(fmt.Sprintf("bit size of %d is greater than word size of %d, not supported",
			bits, bitsPerWord))
	}

	// calculate required space.
	words := wordsRequired(bits, size)
	return &packed{genForbiddenMask(bits), bits, make([]uint64, words), size}
}

func wordsRequired(bits uint, count uint64) (words uint64) {
	words = ((count * uint64(bits)) / bitsPerWord) + 1
	return
}

func genForbiddenMask(bits uint) uint64 {
	return ^((uint64(1) << bits) - 1)
}

// Swap in val at ix and return old value
func (p *packed) Swap(ix uint64, val uint64) (oldval uint64) {
	// XXX this could be more efficient
	oldval = p.Get(ix)
	p.Set(ix, val)
	return
}

func (p *packed) Set(ix uint64, val uint64) {
	if val&p.forbiddenMask != 0 {
		panic(fmt.Sprintf("attempt to store out of range value.  numeric overflow: %x (%x)", (val & p.forbiddenMask), val))
	}
	bitstart := ix * uint64(p.bits)
	word := bitstart / 64
	bitoff := bitstart % 64
	getbits := 64 - (bitoff)
	if getbits > uint64(p.bits) {
		getbits = uint64(p.bits)
	}
	// zero
	p.space[word] =
		((p.space[word] >> (bitoff + getbits)) << (bitoff + getbits)) |
			(p.space[word] << (64 - bitoff) >> (64 - bitoff))

	// or in val
	p.space[word] |= (val << bitoff)

	if uint(getbits) < p.bits {
		remainder := p.bits - uint(getbits)
		p.space[word+1] = ((p.space[word+1] >> remainder) << remainder) | val>>getbits
	}
	return
}

func (p *packed) Get(ix uint64) (val uint64) {
	val, _ = getValFromPackedIx(ix, p.bits, func(off uint64, cnt uint64) ([]uint64, error) {
		return p.space[off : off+cnt], nil
	})
	return
}

func getValFromPackedIx(ix uint64, bits uint, read func(off uint64, cnt uint64) ([]uint64, error)) (val uint64, err error) {
	bitstart := ix * uint64(bits)
	word := bitstart / 64
	bitoff := bitstart % 64
	getbits := 64 - (bitoff)

	if getbits > uint64(bits) {
		getbits = uint64(bits)
	}
	needWords := uint64(1)
	if getbits < uint64(bits) {
		needWords = 2
	}
	words, err := read(word, needWords)
	if err != nil {
		return 0, err
	}

	// now get 'getbits' from 'word' starting at 'bitoff'
	sl := (64 - getbits - bitoff)
	val = (words[0] << sl)
	sr := (64 - getbits)
	val >>= sr
	if getbits < uint64(bits) {
		remainder := uint64(bits) - getbits
		x := (words[1] << (64 - remainder)) >> (64 - remainder)
		val |= x << getbits
	}
	return
}

func (p packed) WriteTo(stream io.Writer) (n int64, err error) {
	h := packedHeader{
		Bits:    uint64(p.bits),
		Size:    p.size,
		Version: qfBitPackedVectorVersion,
	}
	if err = binary.Write(stream, binary.LittleEndian, h); err != nil {
		return
	}
	n, err = writeUintSlice(stream, p.space)
	// is this correct?
	n += int64(unsafe.Sizeof(h))
	return
}

func (p *packed) ReadFrom(stream io.Reader) (n int64, err error) {
	var h packedHeader
	if err = binary.Read(stream, binary.LittleEndian, &h); err != nil {
		return
	}
	if qfBitPackedVectorVersion != h.Version {
		err = fmt.Errorf("invalid file format, bit packed vector version mismatch, got %x, expected %x",
			h.Version, qfBitPackedVectorVersion)
		return
	}
	p.bits = uint(h.Bits)
	p.forbiddenMask = genForbiddenMask(uint(h.Bits))
	p.size = h.Size

	p.space, n, err = readUintSlice(stream)
	n += int64(unsafe.Sizeof(h))

	return
}

type packedDiskReader struct {
	r     io.ReaderAt
	start uint64
	size  uint64
	bits  uint
}

func initPackedDiskReader(stream *os.File) (*packedDiskReader, error) {
	var h packedHeader
	if err := binary.Read(stream, binary.LittleEndian, &h); err != nil {
		return nil, err
	}
	if qfBitPackedVectorVersion != h.Version {
		return nil, fmt.Errorf("invalid file format, bit packed vector version mismatch, got %x, expected %x",
			h.Version, qfBitPackedVectorVersion)
	}
	var words uint64
	err := binary.Read(stream, binary.LittleEndian, &words)
	if err != nil {
		return nil, err
	}
	cur, err := stream.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}
	// seek to end
	_, err = stream.Seek(cur+int64(8*words), io.SeekStart)

	return &packedDiskReader{stream, uint64(cur), uint64(h.Size), uint(h.Bits)}, err
}

func (r packedDiskReader) Read(ix uint64) (val uint64, err error) {
	return getValFromPackedIx(ix, r.bits, func(off uint64, cnt uint64) ([]uint64, error) {
		space := make([]uint64, cnt)
		raw := unsafeUint64SliceToBytes(space)
		n, err := r.r.ReadAt(raw, int64(r.start+off*8))
		if err != nil {
			return nil, err
		}
		if uint64(n) != 8*cnt {
			return nil, fmt.Errorf("short read: %d/%d", n, 8*cnt)
		}

		if !isLittleEndian {
			for i := uint64(0); i < cnt; i++ {
				space[i] = binary.LittleEndian.Uint64(raw[cnt*8 : (cnt+1)*8])
			}
		}

		return space, err
	})
}
