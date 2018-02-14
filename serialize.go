// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved

package qf

import (
	"encoding/binary"
	"fmt"
	"io"
	"unsafe"
)

// qfVersion is a version number for the
// on disk representation format.  Any time incompatible
// changes are made, it is bumped
const qfVersion = uint64(0x0004)

// qfHeader describes a serialized quotient filter
type qfHeader struct {
	// a version number which changes as the storage representation
	// changes
	Version uint64
	// then number of entries in the stored quotient filter
	Entries uint64
	// the number of bits allocated to the quotient filter.  the
	// length of the hash vector on disk will then be 1 << QBits
	QBits uint64
	// the number of bits per bucket of storage represented in the
	// quotient filter.  May be zero if no external storage is in
	// use
	StorageBits uint64
	// whether the quotient filters use bitpacked storage
	BitPacked bool
}

// WriteTo allows the quotient filter to be written to a stream
//
// WARNING: the default storage format is very fast, but not portable
// to architectures of differing word length or endianness
func (qf *Filter) WriteTo(stream io.Writer) (i int64, err error) {
	h := qfHeader{
		Version:     qfVersion,
		Entries:     qf.entries,
		QBits:       uint64(qf.qBits),
		StorageBits: uint64(qf.config.BitsOfStoragePerEntry),
		BitPacked:   qf.config.BitPacked,
	}
	if err = binary.Write(stream, binary.LittleEndian, h); err != nil {
		return
	}
	i += int64(unsafe.Sizeof(h))

	x, err := qf.filter.WriteTo(stream)
	i += x
	if err != nil {
		return
	}

	if qf.storage != nil {
		x, err = qf.storage.WriteTo(stream)
		i += x
		if err != nil {
			return
		}
	}

	return
}

// ReadFrom allows the quotient filter to be read from a stream
//
// WARNING: the default storage format is very fast, but not portable
// to architectures of differing word length or endianness
func (qf *Filter) ReadFrom(stream io.Reader) (i int64, err error) {
	var h qfHeader
	if err = binary.Read(stream, binary.LittleEndian, &h); err != nil {
		return
	}
	// XXX: pretty sure this isn't correct?
	i += int64(unsafe.Sizeof(h))
	if h.Version != qfVersion {
		return i, fmt.Errorf("incompatible file format: version is %d, expected %d",
			h.Version, qfVersion)
	}
	qf.initForQuotientBits(uint(h.QBits))
	n, err := qf.filter.ReadFrom(stream)
	i += n
	if err != nil {
		return
	}

	// read bits

	if h.StorageBits > 0 {
		qf.config.BitsOfStoragePerEntry = uint(h.StorageBits)
		if qf.storage == nil {
			qf.storage = qf.allocfn(0, 0)
		}
		n, err = qf.storage.ReadFrom(stream)
		i += n
		if err != nil {
			return
		}
	}

	return
}
