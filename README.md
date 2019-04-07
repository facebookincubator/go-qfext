# go-qfext

[Quotient filters](https://en.wikipedia.org/wiki/Quotient_filter) are
pretty neat.  They give you the ability to build a time and space efficient
filter that can perform existence checks on a huge number of byte strings.

qfext then is a small and correct quotient filter implementation for
golang with some nifty features.

Specifically qfext:
  1. uses an optimized / inlined [murmur hash](https://en.wikipedia.org/wiki/MurmurHash), 
     but may be configured to use a hash function of your choosing
  2. is about 6 times faster than a [bloom filter]("github.com/willf/bloom") for lookup
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

// bloom filter timing for comparison
BenchmarkBloomFilter-8                               	 5000000	       255 ns/op

// native golang map lookup
BenchmarkMapLookup-8                                 	100000000	        18.6 ns/op
BenchmarkUnpackedQuotientFilterLookup-8              	50000000	        33.8 ns/op

// A lookup in a packed quotient filter is 6.5x faster than a bloom filter, and about
// twice as slow as a native golang map
BenchmarkPackedQuotientFilterLookup-8                	30000000	        38.7 ns/op
BenchmarkQuotientFilterLookupWithExternalStorage-8   	30000000	        43.4 ns/op
```


