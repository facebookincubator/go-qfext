// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved

package main

import (
	qfext ".."
	"bytes"
	"fmt"
)

func main() {
	// helper routines are available to let you size your quotient filter
	// correctly

	fmt.Printf("Example of analyzing size requirements:\n")
	conf := qfext.Config{ExpectedEntries: 1000000000}
	fmt.Printf("A billion entry quotient filter would be loaded at %f percent...\n",
		conf.ExpectedLoading(),
	)
	conf.ExplainIndent("  ")

	fmt.Printf("\nExample of loading and using a small quotient filter:\n")
	data := []string{
		"red", "yellow", "orange", "blue",
	}
	// optimize your quotient filter when you know ahead of time
	// how many entries it will hold.  Otherwise, just use New()
	qf := qfext.NewWithConfig(qfext.Config{
		ExpectedEntries: uint64(len(data)),
		// a bitpacked quotient filter is about 30% less efficient for
		// lookup, however the size reduction can be great, especially
		// for larger quotient filters (where many bits of the hash function
		// are implicitly encoded in the bucked index, the "q" value)
		BitPacked: true,
	})

	for _, color := range data {
		qf.InsertString(color)
	}

	for _, color := range []string{
		"red",
		"orange",
		"yellow",
		"green",
		"blue",
		"indigo",
		"violet",
	} {
		fmt.Printf("%s: %t\n", color, qf.ContainsString(color))
	}

	// Dump the whole quotient filter in textual form
	qf.DebugDump(true)

	// Serialize the quotient filter and report size
	buf := bytes.NewBuffer([]byte{})
	qf.WriteTo(buf)
	fmt.Printf("QF serializes into %d bytes\n", buf.Len())
}
