package cqf

import (
	"fmt"
	"hash/fnv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/willf/bloom"
)

var testStrings []string = []string{
	"cqf",
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
	return
	c := DetermineSize(uint64(len(testStrings)), 4)
	cqf := New(c)

	for _, s := range testStrings {
		cqf.InsertString(s, 0)
		assert.True(t, cqf.ContainsString(s), "%q missing", s)

		q, r := cqf.hash([]byte(s))
		fmt.Printf("\n\nAfter adding %q: %3d|%x\n", s, q, r)
		cqf.DebugDump()
	}
	for _, s := range testStrings {
		if !assert.True(t, cqf.ContainsString(s)) {
			fmt.Printf("%q missing\n", s)
		}
	}
	cqf.DebugDump()
}

func TestCheckHashes(t *testing.T) {
	c := DetermineSize(uint64(len(testStrings)), 4)
	cqf := New(c)
	expected := map[uint64]struct{}{}
	for _, s := range testStrings {
		cqf.DebugDump()
		cqf.InsertString(s, 0)
		if !assert.NoError(t, cqf.CheckConsistency()) {
			cqf.DebugDump()
			return
		}
		hash := fnv.New64()
		hash.Write([]byte(s))
		hv := hash.Sum64()
		expected[hv] = struct{}{}
	}
	assert.NoError(t, cqf.CheckConsistency())
	got := map[uint64]struct{}{}
	cqf.eachHashValue(func(hv uint64) {
		got[hv] = struct{}{}
	})

	for hv, _ := range expected {
		_, found := got[hv]
		if !assert.True(t, found, "missing hash value %x", hv) {
			fmt.Printf("missing %x\n", hv)
		}
	}

	for hv, _ := range got {
		_, found := expected[hv]
		if !assert.True(t, found, "unexpected hash value %x", hv) {
			fmt.Printf("unexpected %x\n", hv)
		}
	}
	assert.Equal(t, len(expected), len(got))
	assert.Equal(t, len(expected), int(cqf.Entries()))

	cqf.DebugDump()
}

func BenchmarkQuotientFilterLookup(b *testing.B) {
	c := DetermineSize(uint64(len(testStrings)), 4)
	cqf := New(c)
	for _, s := range testStrings {
		cqf.InsertString(s, 0)
	}

	numStrings := len(testStrings)

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		cqf.ContainsString(testStrings[n%numStrings])
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
