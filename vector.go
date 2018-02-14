// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved

package qf

import "io"

// VectorAllocateFn allocates a fixed size Vector capable of storing
// 'size' integers of 'bits' width
type VectorAllocateFn func(bits uint, size uint64) Vector

type readFn func(ix uint64) uint64

// Vector stores a fixed size contiguous array of integer data
type Vector interface {
	// Set element ix to the specified value
	Set(ix uint64, val uint64)
	// Swap val in ix and return previous value
	Swap(ix uint64, val uint64) uint64
	// Get the current value stored at element ix
	Get(ix uint64) uint64

	// vectors can be serialized
	io.WriterTo
	io.ReaderFrom
}
