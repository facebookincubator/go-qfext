package qf

import (
	"hash/fnv"
	"math/bits"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/willf/bloom"
)

var testStrings []string = []string{

	" stores",
	"!) can",
	"% loading",
	"(I",
	"(I.e",
	"(fast!)",
	"(fast",
	"(shortcuts",
	") and",
	", at",
	", btw",
	"..  a",
	"...  I",
	"...  for",
	"...  just",
	"...  whereby",
	"...  which",
	".5",
	".5",
	".e",
	"10x",
	"10x",
	"20",
	"200",
	"200",
	"39",
	"5",
	"5",
	"5",
	"5",
	"5",
	"5.5",
	"64",
	"90mb",
	"90mb",
	"95",
	"For",
	"I",
	"I",
	"I",
	"I",
	"I",
	"I",
	"I",
	"I",
	"ID",
	"I’m",
	"I’ve",
	"Now",
	"So",
	"So",
	"a",
	"a",
	"a",
	"a",
	"a",
	"a",
	"a",
	"a",
	"a",
	"a",
	"a",
	"a",
	"a",
	"about",
	"actually",
	"after",
	"ambition",
	"and",
	"and",
	"another",
	"application",
	"approach",
	"array",
	"at",
	"at",
	"be",
	"be",
	"be",
	"benchmarks",
	"bit",
	"bit",
	"bit",
	"bitpacked",
	"bitpacking",
	"bits",
	"bits",
	"bits",
	"bits",
	"bucket",
	"by",
	"can",
	"can",
	"compute",
	"concept",
	"convinced",
	"convinced",
	"corresponds",
	"cost",
	"cost",
	"could",
	"could",
	"could",
	"counts",
	"couple",
	"cqf",
	"cqf",
	"cqf",
	"cqf",
	"data",
	"do",
	"do",
	"domains",
	"domains",
	"e",
	"easy",
	"efficient",
	"efficient",
	"efficiently",
	"entities",
	"entities",
	"entity",
	"entity)...",
	"entries",
	"entry",
	"every",
	"extent",
	"external",
	"external",
	"external",
	"far",
	"faster",
	"faster",
	"fnv",
	"fnv",
	"for",
	"for",
	"for",
	"functional",
	"further",
	"get",
	"gigs",
	"going",
	"got",
	"got",
	"hash",
	"hashing",
	"hashing",
	"have",
	"hours",
	"hours",
	"id",
	"if",
	"immediately",
	"implementation",
	"implementation",
	"in",
	"in",
	"in",
	"in",
	"inside",
	"integer",
	"integer",
	"is",
	"is",
	"is",
	"is",
	"justify",
	"like",
	"like",
	"main",
	"maybe",
	"maybe",
	"measure",
	"memory",
	"memory",
	"mil",
	"mil",
	"million",
	"minor",
	"minor",
	"more",
	"my",
	"my",
	"nearly",
	"nearly",
	"of",
	"of",
	"of",
	"of",
	"of",
	"of",
	"of",
	"out",
	"packed",
	"parallel",
	"per",
	"per",
	"proof",
	"prove",
	"question",
	"rather",
	"remainder",
	"remainder",
	"remainder",
	"run",
	"shit",
	"side",
	"side",
	"single",
	"sized",
	"sloppy",
	"sloppy",
	"slot",
	"slot",
	"so",
	"space",
	"spent",
	"storage",
	"storage",
	"store",
	"structure",
	"than",
	"than",
	"than",
	"that",
	"that",
	"that",
	"the",
	"the",
	"the",
	"the",
	"the",
	"the",
	"the",
	"think",
	"this",
	"throwaway",
	"thus",
	"to",
	"to",
	"to",
	"to",
	"to",
	"to",
	"to",
	"today",
	"today,",
	"ton",
	"trying",
	"two",
	"uint",
	"up",
	"uses",
	"uses",
	"value",
	"value",
	"vector",
	"waaay",
	"want",
	"wasting",
	"what",
	"what",
	"with",
	"with",
	"with",
	"with",
	"work",
	"work",
	"’ll",
	"’m",
	"’m",
	"’ve",
}

func TestBasic(t *testing.T) {
	c := DetermineSize(uint64(len(testStrings)), 4)
	qf := NewWithConfig(c)
	for _, s := range testStrings {
		qf.InsertString(s, 0)
		if !assert.True(t, qf.ContainsString(s), "%q missing", s) {
			return
		}
	}
	for _, s := range testStrings {
		if !assert.True(t, qf.ContainsString(s), "%q missing after construction", s) {
			return
		}
	}
}

// if we don't explicitly size the qf, it should grow on demand
func TestDoubling(t *testing.T) {
	qf := New()
	for _, s := range testStrings {
		qf.InsertString(s, 0)
		assert.True(t, qf.ContainsString(s), "%q missing after insertion", s)
	}
	for _, s := range testStrings {
		assert.True(t, qf.ContainsString(s), "%q missing after construction", s)
	}
}

func TestSizeEstimate(t *testing.T) {
	c := DetermineSize(5500000, 4)
	assert.Equal(t, int(c.BytesRequired()), 98566144)
}

func TestCheckHashes(t *testing.T) {
	c := DetermineSize(uint64(len(testStrings)), 4)
	qf := NewWithConfig(c)
	expected := map[uint64]struct{}{}
	for _, s := range testStrings {
		qf.InsertString(s, 0)
		assert.NoError(t, qf.CheckConsistency())
		hash := fnv.New64()
		hash.Write([]byte(s))
		hv := hash.Sum64()
		expected[hv] = struct{}{}
	}
	assert.NoError(t, qf.CheckConsistency())
	got := map[uint64]struct{}{}
	qf.eachHashValue(func(hv uint64, _ uint) {
		got[hv] = struct{}{}
	})

	for hv, _ := range expected {
		_, found := got[hv]
		assert.True(t, found, "missing hash value %x", hv)
	}

	for hv, _ := range got {
		_, found := expected[hv]
		assert.True(t, found, "unexpected hash value %x", hv)
	}
	assert.Equal(t, len(expected), len(got))
	assert.Equal(t, len(expected), int(qf.Entries()))
}

func TestExternalStorage(t *testing.T) {
	qf := NewWithConfig(Config{
		QBits: 2,
		BitsOfStoragePerEntry: uint8(64 - bits.LeadingZeros64(uint64(len(testStrings)))),
	})
	qf.InsertString("hi mom", 42)
	found, val := qf.LookupString("hi mom")
	assert.True(t, found)
	assert.Equal(t, val, uint64(42))
	last := ""
	for i, s := range testStrings {
		if s != last {
			qf.InsertString(s, uint64(i))
			found, val := qf.LookupString(s)
			assert.True(t, found)
			assert.Equal(t, val, uint64(i))
		}
		last = s
	}
	last = ""
	for i, s := range testStrings {
		if s != last {
			found, val := qf.LookupString(s)
			assert.True(t, found)
			assert.Equal(t, val, uint64(i))
		}
		last = s
	}
}

func BenchmarkQuotientFilterLookup(b *testing.B) {
	c := DetermineSize(uint64(len(testStrings)), 4)
	qf := NewWithConfig(c)
	for _, s := range testStrings {
		qf.InsertString(s, 0)
	}

	numStrings := len(testStrings)

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		qf.ContainsString(testStrings[n%numStrings])
	}
}

func BenchmarkMapLookup(b *testing.B) {
	table := map[string]struct{}{}
	for _, s := range testStrings {
		table[s] = struct{}{}
	}
	numStrings := len(testStrings)
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		_, _ = table[testStrings[n%numStrings]]
	}
}

func BenchmarkBloomFilter(b *testing.B) {
	bf := bloom.NewWithEstimates(uint(len(testStrings)), 0.0001)
	for _, s := range testStrings {
		bf.AddString(s)
	}
	numStrings := len(testStrings)
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		bf.TestString(testStrings[n%numStrings])
	}
}
