// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved

package qf

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/bits"
	"os"
	"strconv"
	"testing"

	murmur "github.com/aviddiviner/go-murmur"
	"github.com/bits-and-blooms/bloom/v3"
	"github.com/stretchr/testify/assert"
)

// testing specific consistency checking
func (qf *Filter) checkConsistency() error {
	if qf.countEntries() != qf.entries {
		return fmt.Errorf("%d items added, only %d found", qf.entries, qf.countEntries())
	}

	// now let's ensure that for every set occupied bit there is a
	// non-zero length run
	usage := map[uint64]uint64{}

	for i := uint64(0); i < qf.size; i++ {
		md := qf.read(i)
		if !md.occupied() {
			continue
		}
		dq := i
		runStart := findStart(dq, qf.size, qf.filter.Get)
		// ok, for bucket dq we've got a run starting at runStart
		for {
			who, used := usage[runStart]
			if used {
				return fmt.Errorf("slot %d used by both dq %d and %d", runStart, dq, who)
			}
			usage[runStart] = dq
			right(&runStart, qf.size)
			md := qf.read(runStart)
			if !md.continuation() {
				break
			}
		}
	}
	if uint64(len(usage)) != qf.entries {
		return fmt.Errorf("records show %d entries in qf, found %d via scanning",
			qf.entries, len(usage))
	}

	return nil
}

var testStrings = []string{
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
	qf := NewWithConfig(Config{
		ExpectedEntries:       uint64(len(testStrings)),
		BitsOfStoragePerEntry: 4,
	})
	for _, s := range testStrings {
		qf.InsertString(s)
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
		qf.InsertString(s)
		qf.checkConsistency()
		if !assert.True(t, qf.ContainsString(s), "%q missing after insertion", s) {
			qf.DebugDump(true)
			return
		}
	}
	for _, s := range testStrings {
		assert.True(t, qf.ContainsString(s), "%q missing after construction", s)
	}
}

func TestSerialization(t *testing.T) {
	for _, packed := range []bool{false, true} {
		qf := NewWithConfig(Config{
			BitPacked: packed,
		})
		for _, s := range testStrings {
			qf.InsertString(s)
			assert.True(t, qf.ContainsString(s), "%q missing after insertion", s)
		}
		var buf bytes.Buffer
		beforeEntries := qf.Len()
		wt, err := qf.WriteTo(&buf)
		assert.NoError(t, err)
		qf = NewWithConfig(Config{
			BitPacked: packed,
		})
		rd, err2 := qf.ReadFrom(&buf)
		assert.NoError(t, err2)
		assert.Equal(t, beforeEntries, qf.Len())
		assert.Equal(t, wt, rd)
		for _, s := range testStrings {
			if !assert.True(t, qf.ContainsString(s), "%q missing after construction", s) {
				return
			}
		}
	}
}

func TestSerializationExternal(t *testing.T) {
	qf := NewWithConfig(Config{
		BitsOfStoragePerEntry: uint(64 - bits.LeadingZeros64(uint64(len(testStrings)))),
	})
	last := ""
	for i, s := range testStrings {
		if s != last {
			qf.InsertStringWithValue(s, uint64(i))
			found, val := qf.LookupString(s)
			assert.True(t, found)
			assert.Equal(t, val, uint64(i))
		}
		last = s
	}
	last = ""

	var buf bytes.Buffer
	wt, err := qf.WriteTo(&buf)
	assert.NoError(t, err)

	// read from should figure out that external storage is present
	qf = New()

	rd, err2 := qf.ReadFrom(&buf)
	assert.NoError(t, err2)
	assert.Equal(t, wt, rd)

	for i, s := range testStrings {
		if s != last {
			found, val := qf.LookupString(s)
			assert.True(t, found)
			assert.Equal(t, uint64(i), val)
		}
		last = s
	}
}

func TestExpectedLoading(t *testing.T) {
	c := Config{ExpectedEntries: 128}
	assert.Equal(t, 50., c.ExpectedLoading())
}

func TestSizeEstimate(t *testing.T) {
	c := Config{ExpectedEntries: 5500000, BitsOfStoragePerEntry: 4}
	assert.Equal(t, 98566144, int(c.BytesRequired()))
}

func TestCheckHashes(t *testing.T) {
	c := Config{ExpectedEntries: uint64(len(testStrings)), BitsOfStoragePerEntry: 4}
	qf := NewWithConfig(c)
	expected := map[uint64]struct{}{}
	for _, s := range testStrings {
		qf.InsertString(s)
		assert.NoError(t, qf.checkConsistency())
		hv := murmur.MurmurHash64A([]byte(s), 0)
		expected[hv] = struct{}{}
	}
	assert.NoError(t, qf.checkConsistency())
	got := map[uint64]struct{}{}
	qf.eachHashValue(func(hv uint64, _ uint64) {
		got[hv] = struct{}{}
	})

	for hv := range expected {
		_, found := got[hv]
		assert.True(t, found, "missing hash value %x", hv)
	}

	for hv := range got {
		_, found := expected[hv]
		assert.True(t, found, "unexpected hash value %x", hv)
	}
	assert.Equal(t, len(expected), len(got))
	assert.Equal(t, len(expected), int(qf.Len()))
}

func TestExternalStorage(t *testing.T) {
	qf := NewWithConfig(Config{
		BitsOfStoragePerEntry: uint(64 - bits.LeadingZeros64(uint64(len(testStrings)))),
	})
	qf.InsertStringWithValue("hi mom", 42)
	found, val := qf.LookupString("hi mom")
	assert.True(t, found)
	assert.Equal(t, val, uint64(42))
	last := ""
	for i, s := range testStrings {
		if s != last {
			qf.InsertStringWithValue(s, uint64(i))
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

func writeQFToTempFile(qf *Filter) (string, error) {
	f, err := ioutil.TempFile("", "qfext_test")
	if err != nil {
		return "", err
	}
	name := f.Name()
	_, err = qf.WriteTo(f)
	f.Close()
	return name, err
}

func TestReadOnlyFromDisk(t *testing.T) {
	for _, packed := range []bool{ /*false,*/ true} {
		qf := NewWithConfig(Config{
			BitsOfStoragePerEntry: uint(64 - bits.LeadingZeros64(uint64(len(testStrings)))),
			BitPacked:             packed,
		})
		// first, populate quotient filter
		last := ""
		for i, s := range testStrings {
			if s != last && s == "actually" {
				qf.InsertStringWithValue(s, uint64(i))
				found, val := qf.LookupString(s)
				assert.True(t, found)
				assert.Equal(t, val, uint64(i))
			}
			last = s
		}
		// verify, presence of all values in quotient filter
		last = ""
		for i, s := range testStrings {
			if s != last && s == "actually" {
				found, val := qf.LookupString(s)
				assert.True(t, found)
				assert.Equal(t, val, uint64(i))
			}
			last = s
		}
		// write to disk
		name, err := writeQFToTempFile(qf)
		defer func() {
			os.Remove(name)
		}()
		assert.NoError(t, err)

		qfr, err := OpenReadOnlyFromPath(name)
		if !assert.NoError(t, err) {
			return
		}
		defer func() {
			qfr.Close()
		}()

		// finally verify presence of all values in quotient filter via read only from disk
		last = ""
		for i, s := range testStrings {
			if s != last && s == "actually" {
				found, val := qfr.LookupString(s)
				if assert.True(t, found) {
					assert.Equal(t, uint64(i), val)
				}
			}
			last = s
		}
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

func BenchmarkUnpackedFilterLookup(b *testing.B) {
	c := Config{BitPacked: false, ExpectedEntries: uint64(len(testStrings))}
	qf := NewWithConfig(c)
	for _, s := range testStrings {
		qf.InsertString(s)
	}

	numStrings := len(testStrings)

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		qf.ContainsString(testStrings[n%numStrings])
	}
}

func createQFFilterOnDiskForBenchmarking(packed bool) (string, *Disk, error) {
	c := Config{BitPacked: false, ExpectedEntries: uint64(len(testStrings))}
	qf := NewWithConfig(c)
	for _, s := range testStrings {
		qf.InsertString(s)
	}
	name, err := writeQFToTempFile(qf)
	if err != nil {
		return name, nil, err
	}
	ext, err := OpenReadOnlyFromPath(name)
	if err != nil {
		return name, nil, err
	}
	return name, ext, nil
}

func BenchmarkUnpackedDiskFilterLookup(b *testing.B) {
	name, ext, err := createQFFilterOnDiskForBenchmarking(false)
	defer func() {
		os.Remove(name)
	}()
	if !assert.NoError(b, err) {
		return
	}
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		ext.ContainsString(testStrings[n%len(testStrings)])
	}
}

func BenchmarkPackedDiskFilterLookup(b *testing.B) {
	name, ext, err := createQFFilterOnDiskForBenchmarking(true)
	defer func() {
		os.Remove(name)
	}()
	if !assert.NoError(b, err) {
		return
	}
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		ext.ContainsString(testStrings[n%len(testStrings)])
	}
}

func BenchmarkUnpackedFilterLookupWithFNV(b *testing.B) {
	c := Config{BitPacked: false, ExpectedEntries: uint64(len(testStrings)), HashFn: fnvhash}
	qf := NewWithConfig(c)

	for _, s := range testStrings {
		qf.InsertString(s)
	}

	numStrings := len(testStrings)

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		qf.ContainsString(testStrings[n%numStrings])
	}
}

func BenchmarkPackedFilterLookup(b *testing.B) {
	c := Config{BitPacked: true, ExpectedEntries: uint64(len(testStrings))}
	qf := NewWithConfig(c)
	for _, s := range testStrings {
		qf.InsertString(s)
	}

	numStrings := len(testStrings)

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		qf.ContainsString(testStrings[n%numStrings])
	}
}

func newQFForRWBench(c Config) *Filter {
	qf := NewWithConfig(c)
	buf := make([]byte, 8)
	for n := 0; n < 100000; n++ {
		x := strconv.AppendInt(buf[:0], int64(n), 10)
		qf.Insert(x)
	}
	return qf
}

func BenchmarkUnpackedSerialize(b *testing.B) {
	qf := newQFForRWBench(Config{BitPacked: false})
	buf := bytes.NewBuffer(nil)

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		buf.Reset()
		qf.WriteTo(buf)
	}
}

func BenchmarkUnpackedDeserialize(b *testing.B) {
	c := Config{BitPacked: false}
	qf := newQFForRWBench(c)
	buf := bytes.NewBuffer(nil)
	qf.WriteTo(buf)
	qfr := NewWithConfig(c)

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		qfr.ReadFrom(bytes.NewBuffer(buf.Bytes()))
	}
}

func BenchmarkUnpackedExternalOpen(b *testing.B) {
	c := Config{BitPacked: false}
	qf := newQFForRWBench(c)
	name, err := writeQFToTempFile(qf)
	if !assert.NoError(b, err) {
		return
	}
	defer func() {
		os.Remove(name)
	}()

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		ext, err := OpenReadOnlyFromPath(name)
		assert.NoError(b, err)
		ext.Close()
	}
}

func BenchmarkPackedSerialize(b *testing.B) {
	qf := newQFForRWBench(Config{BitPacked: true})
	buf := bytes.NewBuffer(nil)

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		buf.Reset()
		qf.WriteTo(buf)
	}
}

func BenchmarkPackedDeserialize(b *testing.B) {
	c := Config{BitPacked: true}
	qf := newQFForRWBench(c)
	buf := bytes.NewBuffer(nil)
	qf.WriteTo(buf)

	b.ResetTimer()

	qfr := NewWithConfig(c)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		qfr.ReadFrom(bytes.NewBuffer(buf.Bytes()))
	}
}

func BenchmarkPackedFilterLookupWithFNV(b *testing.B) {
	c := Config{BitPacked: true, ExpectedEntries: uint64(len(testStrings)), HashFn: fnvhash}
	qf := NewWithConfig(c)

	for _, s := range testStrings {
		qf.InsertString(s)
	}

	numStrings := len(testStrings)

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		qf.ContainsString(testStrings[n%numStrings])
	}
}

func BenchmarkUnpackedFilterLookupWithExternalStorage(b *testing.B) {
	c := Config{BitPacked: false, ExpectedEntries: uint64(len(testStrings)), BitsOfStoragePerEntry: 15}
	qf := NewWithConfig(c)
	for i, s := range testStrings {
		qf.InsertStringWithValue(s, uint64(i))
	}

	numStrings := len(testStrings)

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		qf.LookupString(testStrings[n%numStrings])
	}
}

func BenchmarkLoading(b *testing.B) {
	qf := NewWithConfig(Config{ExpectedEntries: uint64(b.N)})

	b.ResetTimer()
	buf := make([]byte, 8)
	for n := 0; n < b.N; n++ {
		x := strconv.AppendInt(buf[:0], int64(n), 10)
		qf.Insert(x)
	}
}
