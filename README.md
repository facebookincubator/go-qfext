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
  2. is about 6 times faster than a popular [bloom filter implementation](https://github.com/willf/bloom) for lookup
  3. supports "external storage".  You may associate a variable width integer with each
     key.
  4. supports direct reading of quotient filter from disk for situation where runtime performance is less important
     than ram usage

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

On a modern linux machine laptop:

```
$ go test --bench="Bench"

// bloom filter timing
BenchmarkBloomFilter-24					 3000000	       460 ns/op

// native golang map lookup
BenchmarkMapLookup-24					50000000	        23.5 ns/op

// a lookup in a non-bitpacked quotient filter is only about 50% slower than native
// golang maps
BenchmarkUnpackedFilterLookup-24			50000000	        35.4 ns/op

// Bitpacking costs a bit but saves on space.  The larger the filter, the more you
// save.
BenchmarkPackedFilterLookup-24				20000000	        55.3 ns/op

// External storage uses the same representation as the filter itself
BenchmarkUnpackedFilterLookupWithExternalStorage-24    	30000000	        40.3 ns/op

// quotient filter loading assuming a pre-sized quotient filter (no doubling)
BenchmarkLoading-24                                    	 20000000          165 ns/op

// opening a 3mb quotient filter, loading into memory
BenchmarkPackedDeserialize-24				    2000	   1138300 ns/op	 3154209 B/op	      16 allocs/op

// opening the same 4mb quotient filter but leaving the filter on disk
BenchmarkUnpackedExternalOpen-24                       	  200000        8355 ns/op	     600 B/op	      17 allocs/op
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

