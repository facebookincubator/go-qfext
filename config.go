package qf

import "fmt"

// MinQBits is the initial number of quotient bits when no explicit
// configuration is provided, and the minimum number of qbits supported
//
// implementation note:  MinQBits must be greater than 3 as we require
// 3 bits for quotient filter book keeping
const MinQBits = 4

// DetermineSize generates a Config struct appropriate for a
// quotient filter that can hold numberOfEntites, while remaining under
// MaxLoadingFactor
func DetermineSize(numberOfEntries uint, bitsOfStoragePerEntry uint) Config {
	x := uint(1)
	bits := uint(0)
	for (float64(x) * MaxLoadingFactor) < float64(numberOfEntries) {
		x <<= 1
		bits++
	}
	return Config{
		QBits:                 bits,
		BitsOfStoragePerEntry: bitsOfStoragePerEntry,
	}
}

// RepresentationConfig configures behaviors which affect disk representation
// including storage format and hash function.  This configuration must be
// supplied at deserialization time
type RepresentationConfig struct {
	RemainderAllocFn VectorAllocateFn
	StorageAllocFn   VectorAllocateFn
	HashFn           HashFn
}

// DefaultRepresentationConfig is the configuration used by default for
// remainder and storage representation as well as hash function.
// By default we use a bit-packed in memory (and on disk) representation
// to conserve space at a minor computational cost, and a 64 bit murmur
// 2 hash function
var DefaultRepresentationConfig = RepresentationConfig{
	RemainderAllocFn: BitPackedVectorAllocate,
	StorageAllocFn:   BitPackedVectorAllocate,
	HashFn:           murmurhash64,
}

// Config controls the behavior of the quotient filter
type Config struct {
	// The number of bits to use for quotient representation
	QBits uint
	// The number of bits of storage to alloate and manage per
	// entry.
	BitsOfStoragePerEntry uint
	// Configuration of remainder+data representation as well
	// as hash function
	Representation RepresentationConfig
}

// ExpectedLoading reports the expected percentage loading given the
// sizing and number of entries.
func (c *Config) ExpectedLoading(expectedNumberOfEntries uint) float64 {
	return 100. * float64(expectedNumberOfEntries) / float64(c.BucketCount())
}

// BytesRequired reports the approximate amount of space required to represent
// the quotient filter on disk or in ram (assuming bit packing).
func (c *Config) BytesRequired() uint {
	bitsPerEntry := (64 - uint(c.QBits)) + 3 + uint(c.BitsOfStoragePerEntry)
	return c.BucketCount() * bitsPerEntry / 8
}

// BucketCount reports the number of hash buckets that will be allocated
// given the quotient bits.
func (c *Config) BucketCount() uint {
	return 1 << (uint(c.QBits))
}

// ExplainIndent will print an indented summary of the configuration to stdout
func (c *Config) ExplainIndent(indent string) {
	fmt.Printf("%s%2d bits configured for quotient (%d buckets)\n", indent, c.QBits, c.BucketCount())
	fmt.Printf("%s%2d bits needed per bucket for remainder\n", indent, BitsPerWord-c.QBits)
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
