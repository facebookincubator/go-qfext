// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved

package qf

import (
	"fmt"
	"io"
	"reflect"
	"unsafe"
)

// backing is a function that allows symmetric access to quotient
// filter data
type backing func(offset uint64) (value uint64, err error)

type ramBacking []uint64

func (v *ramBacking) Set(ix uint64, val uint64) error {
	(*v)[ix] = val
	return nil
}

func (v ramBacking) Get(ix uint64) (val uint64, err error) {
	if ix >= uint64(len(v)) {
		return 0, fmt.Errorf("index %d is greater than ramBacking size of %d\n", ix, len(v))
	}
	return v[ix], nil
}

type diskBacking struct {
	start uint64
	f     io.ReaderAt
}

func (b diskBacking) Func(offset uint64) (uint64, error) {
	var val [8]byte
	n, err := b.f.ReadAt(val[:8], int64(b.start+offset*8))
	if err != nil {
		return 0, fmt.Errorf("failed to read from qf backing: %s", err)
	}
	if n != 8 {
		return 0, fmt.Errorf("short read: %d/8", n)
	}
	uip := (*uint64)(unsafe.Pointer((*(*reflect.SliceHeader)(unsafe.Pointer(&val))).Data))
	// XXX: endian-ness conversion?
	return *uip, nil
}
