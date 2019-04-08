package qf

import (
	"encoding/binary"
	"fmt"
	"io"
	"unsafe"
)

// RepresentationVersion is a version number for the
// on disk representation format.  Any time incompatible
// changes are made, it is bumped
const RepresentationVersion = uint64(0x0003)

// Header describes a serialized quotient filter
type Header struct {
	// How many values are stored in the filter
	Entries uint
	// The number of bits used for the quotient
	QBits uint
}

// WriteTo allows the quotient filter to be written to a stream
//
// WARNING: the default storage format is very fast, but not portable
// to architectures of differing word length or endianness
func (qf *QF) WriteTo(stream io.Writer) (i int64, err error) {
	if err = binary.Write(stream, binary.LittleEndian, RepresentationVersion); err != nil {
		return
	}
	i += int64(unsafe.Sizeof(RepresentationVersion))
	if err = binary.Write(stream, binary.LittleEndian, uint64(qf.entries)); err != nil {
		return
	}
	i += int64(unsafe.Sizeof(uint64(qf.entries)))
	if err = binary.Write(stream, binary.LittleEndian, uint64(qf.qBits)); err != nil {
		return
	}
	i += int64(unsafe.Sizeof(uint64(qf.qBits)))
	if err = binary.Write(stream, binary.LittleEndian, uint64(qf.config.BitsOfStoragePerEntry)); err != nil {
		return
	}
	i += int64(unsafe.Sizeof(uint64(qf.config.BitsOfStoragePerEntry)))

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
func (qf *QF) ReadFrom(stream io.Reader) (i int64, err error) {
	var entries, qBits, storageBits uint64
	var ver uint64
	if err = binary.Read(stream, binary.LittleEndian, &ver); err != nil {
		return
	}
	i += int64(unsafe.Sizeof(ver))
	if ver != RepresentationVersion {
		return i, fmt.Errorf("representation version is %d, expected %d",
			ver, RepresentationVersion)
	}
	if err = binary.Read(stream, binary.LittleEndian, &entries); err != nil {
		return
	}
	i += int64(unsafe.Sizeof(entries))
	if err = binary.Read(stream, binary.LittleEndian, &qBits); err != nil {
		return
	}
	i += int64(unsafe.Sizeof(qBits))
	if err = binary.Read(stream, binary.LittleEndian, &storageBits); err != nil {
		return
	}
	i += int64(unsafe.Sizeof(storageBits))

	var nqf QF
	nqf.hashfn = qf.hashfn
	nqf.config = qf.config
	nqf.entries = uint(entries)
	nqf.initForQuotientBits(uint(qBits))

	nqf.filter = qf.config.Representation.RemainderAllocFn(0, 0)
	n, err := nqf.filter.ReadFrom(stream)
	i += n
	if err != nil {
		return
	}

	// read bits

	if storageBits > 0 {
		nqf.config.BitsOfStoragePerEntry = uint(storageBits)
		nqf.storage = qf.config.Representation.StorageAllocFn(0, 0)
		n, err = nqf.storage.ReadFrom(stream)
		i += n
		if err != nil {
			return
		}
	}

	// overwrite myself, preserve the representation parameters
	// in the instance.
	rep := qf.config.Representation
	*qf = nqf
	qf.config.Representation = rep

	return
}
