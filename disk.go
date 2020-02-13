// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved

package qf

import (
	"encoding/binary"
	"fmt"
	"os"
	"unsafe"
)

type extReader interface {
	Read(ix uint64) (val uint64, err error)
}

// Disk is a read-only quotient filter that interacts with a
// quotient filter on disk without loading it into RAM
type Disk struct {
	entries                 uint64
	size                    uint64
	hashfn                  HashFn
	rBits                   uint
	rMask                   uint64
	f                       *os.File
	filterRead, storageRead extReader
	storageBits             uint
}

// OpenReadOnlyFromFile initializes a read only quotient filter
// from disk
func OpenReadOnlyFromPath(path string) (*Disk, error) {
	rdr, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	// read header
	var h qfHeader
	if err = binary.Read(rdr, binary.LittleEndian, &h); err != nil {
		return nil, err
	}
	var ext Disk
	ext.entries = h.Entries
	ext.rBits, ext.rMask, ext.size = initForQuotientBits(uint(h.QBits))
	ext.storageBits = uint(h.StorageBits)
	if h.BitPacked {
		ext.filterRead, err = initPackedDiskReader(rdr)
		if err != nil {
			return nil, err
		}
		if h.StorageBits > 0 {
			ext.storageRead, err = initPackedDiskReader(rdr)
			if err != nil {
				return nil, err
			}
		}
	} else {
		ext.filterRead, err = initUnpackedDiskReader(rdr)
		if err != nil {
			return nil, err
		}
		if h.StorageBits > 0 {
			ext.storageRead, err = initUnpackedDiskReader(rdr)
			if err != nil {
				return nil, err
			}
		}
	}
	// XXX: handle variable hash functions
	ext.hashfn = murmurhash64
	return &ext, nil
}

// StorageBits reports the number of bits of integer storage associated
// with each entry in the quotient filter
func (ext *Disk) StorageBits() uint {
	return ext.storageBits
}

// HasStorage is true when a non-zero amount of integer storage is tracked
// along with each entry in the quotient filter
func (ext *Disk) HasStorage() bool {
	return ext.storageBits > 0
}

// Close the file handle associated with the disk based quotient filter
func (ext *Disk) Close() error {
	if ext.f != nil {
		return ext.f.Close()
	}
	return nil
}

// Len reports the number of entries in the quotient filter
func (ext *Disk) Len() uint64 {
	return ext.entries
}

// Contains checks whether the byte string is stored within the quotient filter
func (ext *Disk) Contains(v []byte) bool {
	found, _ := ext.Lookup(v)
	return found
}

// Contains checks whether the string is stored within the quotient filter
func (ext *Disk) ContainsString(s string) bool {
	found, _ := ext.Lookup(*(*[]byte)(unsafe.Pointer(&s)))
	return found
}

// Lookup checks whether the byte string is stored within the quotient filter and
// returns a boolean indicating its presence and external integer data if applicable
func (ext *Disk) Lookup(key []byte) (bool, uint64) {
	dq, dr := hash(ext.hashfn, key, ext.rBits, ext.rMask)

	var filterFn, storageFn readFn
	filterFn = func(v uint64) uint64 {
		x, err := ext.filterRead.Read(v)
		if err != nil {
			panic(fmt.Sprintf("error: %s", err))
		}
		return x
	}
	if ext.storageRead != nil {
		storageFn = func(v uint64) uint64 {
			x, err := ext.storageRead.Read(v)
			if err != nil {
				panic(fmt.Sprintf("error: %s", err))
			}
			return x
		}
	}
	return lookupByHash(dq, dr, ext.size, filterFn, storageFn)
}

// LookupString is like Lookup, but for strings
func (ext *Disk) LookupString(key string) (bool, uint64) {
	return ext.Lookup(*(*[]byte)(unsafe.Pointer(&key)))
}
