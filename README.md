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
  1. uses an optimized / inlined [murmur hash](https://en.wikipedia.org/wiki/MurmurHash), 
     but may be configured to use a hash function of your choosing
  2. is about 6 times faster than a popular [bloom filter implementation]("github.com/willf/bloom") for lookup
  3. supports "external storage".  You may associate a variable width integer with each
     key.
  4. supports pluggable storage - Both a fast bit packed implementation (non-portable) and
     a unpacked (portable) implementation is implemented.  This storage is used both for 
     the remainder store and the external storage 

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

## Performance - Time

On a modern osx laptop:

```
$ go test --bench="Bench"

// bloom filter timing
BenchmarkBloomFilter-8                               	 5000000	       248 ns/op

// native golang map lookup
BenchmarkMapLookup-8                                 	100000000	        17.5 ns/op

// a lookup in a non-bitpacked quotient filter is only about 70% slower than native
// golang maps
BenchmarkUnpackedQuotientFilterLookup-8              	50000000	        29.0 ns/op

// Bitpacking costs a bit but saves on space.  The larger the filter, the more you
// save.
BenchmarkPackedQuotientFilterLookup-8                	50000000	        37.6 ns/op

// External storage uses the same representation as the filter itself
BenchmarkQuotientFilterLookupWithExternalStorage-8   	30000000	        43.5 ns/op

// quotient filter loading assuming a pre-sized quotient filter (no doubling)
BenchmarkLoading-8                                   	10000000	       188 ns/op
```

## Performance - Memory

This package is optimized for large models that are expensive to
generate.  The "packed" storage implementation bitpacks the model into
contiguous storage.  During loading we directly read the entire file
into contigous ram, so you pay for exactly your model size and loading
time is basically limited by your disk.

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

