package qf

import "io"

// VectorAllocateFn allocates a fixed size Vector capable of storing
// 'size' integers of 'bits' width
type VectorAllocateFn func(bits uint, size uint) Vector

// Vector stores a fixed size contiguous array of integer data
type Vector interface {
	// Set element ix to the specified value
	Set(ix uint, val uint)
	// Swap val in ix and return previous value
	Swap(ix uint, val uint) uint
	// Get the current value stored at element ix
	Get(ix uint) uint

	// vectors can be serialized
	io.WriterTo
	// Read will construct a new vector from the specified io.Reader
	// and return it
	ReadFrom(r io.Reader) (int64, error)
}
