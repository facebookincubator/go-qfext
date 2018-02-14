// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved

package qf

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

type unpacked []uint64

var _ Vector = (*unpacked)(nil)

// UnpackedVectorAllocate allocates non-bitpacked storage with a portable
// serialization format (i.e. between architectures)
func UnpackedVectorAllocate(bits uint, size uint64) Vector {
	if bits > bitsPerWord {
		panic(fmt.Sprintf("bit size of %d is greater than word size of %d, not supported",
			bits, bitsPerWord))
	}
	arr := make(unpacked, size)
	return &arr
}

func (v *unpacked) Set(ix uint64, val uint64) {
	(*v)[ix] = val
}

func (v *unpacked) Swap(ix uint64, val uint64) (oldval uint64) {
	(*v)[ix], oldval = val, (*v)[ix]
	return
}

func (v *unpacked) Get(ix uint64) (val uint64) {
	return (*v)[ix]
}

// unpacked format on disk is:
// 64 bit len
// len x 64 bit unsigned integers
func (v unpacked) WriteTo(w io.Writer) (n int64, err error) {
	return writeUintSlice(w, v)
}

func (v *unpacked) ReadFrom(r io.Reader) (n int64, err error) {
	*v, n, err = readUintSlice(r)
	return
}

type unpackedDiskReader struct {
	r     io.ReaderAt
	start uint64
	size  uint64
}

func initUnpackedDiskReader(rdr *os.File) (*unpackedDiskReader, error) {
	var sz uint64
	err := binary.Read(rdr, binary.LittleEndian, &sz)
	if err != nil {
		return nil, err
	}
	cur, err := rdr.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}
	// seek to end
	_, err = rdr.Seek(cur+int64(8*sz), io.SeekStart)
	return &unpackedDiskReader{rdr, uint64(cur), uint64(sz)}, err
}

func (r unpackedDiskReader) Read(ix uint64) (val uint64, err error) {
	var data [8]byte
	off := int64(ix*8 + r.start)
	n, err := r.r.ReadAt(data[:8], off)
	if err != nil {
		return 0, err
	}
	if n != 8 {
		return 0, fmt.Errorf("short read: %d/8", n)
	}
	val = binary.LittleEndian.Uint64(data[:8])
	return
}
