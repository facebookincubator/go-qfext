package qf

import "fmt"

func DetermineSize(numberOfEntries uint, bitsOfStoragePerEntry uint) Config {
	x := uint(1)
	bits := uint(0)
	for x < (numberOfEntries * 2) {
		x <<= 1
		bits++
	}
	return Config{
		ExpectedNumberOfEntries: numberOfEntries,
		QBits: bits,
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

var DefaultRepresentationConfig = RepresentationConfig{
	RemainderAllocFn: BitPackedVectorAllocate,
	StorageAllocFn:   BitPackedVectorAllocate,
	HashFn:           murmurhash64,
}

type Config struct {
	ExpectedNumberOfEntries uint
	QBits                   uint
	BitsOfStoragePerEntry   uint
	Representation          RepresentationConfig
}

func (c *Config) ExpectedLoading() float64 {
	return 100. * float64(c.ExpectedNumberOfEntries) / float64(c.BucketCount())
}

func (c *Config) BytesRequired() uint {
	bitsPerEntry := (64 - uint(c.QBits)) + 3 + uint(c.BitsOfStoragePerEntry)
	return c.BucketCount() * bitsPerEntry / 8
}

func (c *Config) BucketCount() uint {
	return 1 << (uint(c.QBits))
}

func (c *Config) Explain() {
	fmt.Printf("For %d expected entities...\n", c.ExpectedNumberOfEntries)
	fmt.Printf("%d bits needed for quotient (%d buckets)\n", c.QBits, c.BucketCount())
	fmt.Printf("%d bits needed per bucket for remainder\n", 64-c.QBits)
	fmt.Printf("3 bits metadata per bucket\n")
	fmt.Printf("%d bits external storage\n", c.BitsOfStoragePerEntry)
	fmt.Printf("%0.2f%% loading expected\n", c.ExpectedLoading())
	fmt.Printf("%d bytes required\n", c.BytesRequired())
}
