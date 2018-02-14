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

func (ext *Disk) Close() error {
	if ext.f != nil {
		return ext.f.Close()
	}
	return nil
}

func (ext *Disk) Len() uint64 {
	return ext.entries
}

func (ext *Disk) Contains(v []byte) bool {
	found, _ := ext.Lookup(v)
	return found
}

func (ext *Disk) ContainsString(s string) bool {
	found, _ := ext.Lookup(*(*[]byte)(unsafe.Pointer(&s)))
	return found
}

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

func (ext *Disk) LookupString(key string) (bool, uint64) {
	return ext.Lookup(*(*[]byte)(unsafe.Pointer(&key)))
}
