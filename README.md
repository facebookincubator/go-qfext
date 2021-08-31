# go-qfext

[Quotient filters](https://en.wikipedia.org/wiki/Quotient_filter) are
pretty neat.  They are a probabalistic data structure that give you
the ability to build a time and space efficient filter that can
perform existence checks on a huge number of byte strings, with a very
low false positive rate (effectively the hash collision probability).

More technically, a quotient filter is a hash table where we store
only hash values.  Further, we use a variable number of bits from the
hash value as the hash bucket.  This means table size is always a
power of two.  Finally, rather than creating linked lists for overflow
runs, we encode runs directly into neighboring buckets using a scheme
that costs 3 bits.

qfext is a small and correct quotient filter implementation for
golang with some nifty features.

Specifically qfext:
  1. uses an optimized / inlined [murmur hash](https://en.wikipedia.org/wiki/MurmurHash).
  2. is about 4 times faster than a popular [bloom filter implementation](https://github.com/bits-and-blooms/bloom/v3) for lookup
  3. supports "external storage".  You may associate a variable width integer with each key.
  4. supports direct reading of quotient filter from disk for situation where runtime performance is less important than ram usage

## License

This package is available under the MIT License, Copyright (c) Facebook, See the LICENSE file.

## Example Usage

```
func main() {
  qf := qfext.New()
  qf.InsertString("hi mom")
  exists := qf.ContainsString("hi mom")	
  if exists {
    fmt.Printf("maternal salutation exists\n")
  }
}
```

## Performance - Compute

On a MacBook Pro (13-inch, M1, 2020)

```
$ go test -run=xxx -bench=.

goos: darwin
goarch: arm64
pkg: github.com/facebookincubator/go-qfext

// bloom filter timing
BenchmarkBloomFilter-8                               	13851920	        74.47 ns/op

// native golang map lookup
BenchmarkMapLookup-8                                 	130050450	         9.373 ns/op

// a lookup in a non-bitpacked quotient filter is only about 50% slower than native
// golang maps
BenchmarkUnpackedFilterLookup-8                      	58248184	        20.43 ns/op

// Bitpacking costs a bit but saves on space.  The larger the filter, the more you
// save.
BenchmarkPackedFilterLookup-8                        	38458353	        30.95 ns/op

// External storage uses the same representation as the filter itself
BenchmarkUnpackedFilterLookupWithExternalStorage-8   	47611646	        25.59 ns/op

// quotient filter loading assuming a pre-sized quotient filter (no doubling)
BenchmarkLoading-8                                   	14826261	       155.8 ns/op

// loading a 2mb serialized quotient filter into memory
BenchmarkUnpackedDeserialize-8                       	   14174	     83156 ns/op	 2097313 B/op	       6 allocs/op

// loading a 2mb serialized quotient directly from a file (subsequent reads may cause paging / file reads)
BenchmarkUnpackedExternalOpen-8                      	  134208	      9182 ns/op	     424 B/op	       9 allocs/op

```

## Performance - Memory

This package is optimized for large models that are expensive to
generate.  The "packed" storage implementation bitpacks the model into
contiguous storage.  During loading we directly read the entire file
into contigous ram, so you pay for exactly your model size and loading
time is basically limited by your disk.  

NOTE: the disk format of binary data is little endian, reading and writing
on a big endian machine will be slower

The formula for determining how much memory is required is:
  1. determine the number of entries you will store
  2. find the smallest n such that 2^n is greater than 65% of #1 
  3. 3 + 64 - n is your per entry size (3 bits of overhead, and then
     you don't need to store the first n bits, they are implied by
     the hash bucket.

For example (acutally, from the example/ directory), a billion entry quotient filter would...
```
a billion entry bloom filter would be loaded at 46.566129 percent...
  31 bits configured for quotient (2147483648 buckets)
  33 bits needed per bucket for remainder
   3 bits metadata per bucket
   0 bits external storage
     9.00 GB data storage size expected
```

