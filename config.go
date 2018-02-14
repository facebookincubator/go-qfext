// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved

package qf

import "fmt"

// minQBits is the initial number of quotient bits when no explicit
// configuration is provided, and the minimum number of qbits supported
//
// implementation note:  minQBits must be greater than 3 as we require
// 3 bits for quotient filter book keeping
const minQBits = 4

// Config controls the behavior of the quotient filter
type Config struct {
	// The number of bits of storage to allocate and manage per
	// entry.
	BitsOfStoragePerEntry uint
	// BitPacked, when true, will use a bitpacked storage format
	// which is slightly less efficient computationally but results
	// in a smaller quotient filter, especially for larger numbers
	// of entries
	BitPacked bool
	// ExpectedEntries may be provided to pre-size a quotient filter
	// which can reduces time during batch loading.  The quotient
	// filter will be automatically sized to be just large enough to
	// hold the expected number of entries without exceeding a reasonable
	// loading factor
	ExpectedEntries uint64
	// HashFn may be specified to over-ride the default used by the
	// implementation (64 bit murmur hash).  When over-ridded, caller must
	// take care that when a quotient filter is loaded the hash function
	// is set to the same hash function used when populating the quotient
	// filter
	HashFn HashFn
}

// ExpectedLoading reports the expected percentage loading given the
// number of entries specified
func (c *Config) ExpectedLoading() float64 {
	return 100. * float64(c.ExpectedEntries) / float64(c.BucketCount())
}

// BytesRequired reports the approximate amount of space required to represent
// the quotient filter on disk or in ram (assuming bit packing).
func (c *Config) BytesRequired() uint {
	bitsPerEntry := (64 - c.QBits()) + 3 + uint(c.BitsOfStoragePerEntry)
	return c.BucketCount() * bitsPerEntry / 8
}

// BucketCount reports the number of hash buckets that will be allocated
// given the quotient bits.
func (c *Config) BucketCount() uint {
	return 1 << c.QBits()
}

// QBits returns the number of bits of the hash balue that will be used
// to determine the hash bucket
func (c *Config) QBits() uint {
	x := uint(1)
	bits := uint(0)
	for (float64(x) * MaxLoadingFactor) < float64(c.ExpectedEntries) {
		x <<= 1
		bits++
	}
	if bits < minQBits {
		bits = minQBits
	}
	return bits
}

// ExplainIndent will print an indented summary of the configuration to stdout
func (c *Config) ExplainIndent(indent string) {
	fmt.Printf("%s%2d bits configured for quotient (%d buckets)\n", indent, c.QBits(), c.BucketCount())
	fmt.Printf("%s%2d bits needed per bucket for remainder\n", indent, bitsPerWord-c.QBits())
	fmt.Printf("%s%2d bits metadata per bucket\n", indent, 3)
	fmt.Printf("%s%2d bits external storage\n", indent, c.BitsOfStoragePerEntry)
	fmt.Printf("%s   %s storage size expected\n", indent, humanBytes(c.BytesRequired()))
}

// Explain will print a summary of the configuration to stdout
func (c *Config) Explain() {
	c.ExplainIndent("")
}

func humanBytes(bytes uint) string {
	v := float64(bytes)
	suffix := "bytes"
	if v > 1024 {
		v /= 1024.
		suffix = "KB"
		if v > 1024. {
			suffix = "MB"
			v /= 1024.0
			if v > 1024. {
				suffix = "GB"
				v /= 1024.
			}
		}
	}
	if v < 10 {
		return fmt.Sprintf("%0.2f %s", v, suffix)
	} else if v < 100 {
		return fmt.Sprintf("%0.1f %s", v, suffix)
	} else {
		return fmt.Sprintf("%0.0f %s", v, suffix)
	}
}
