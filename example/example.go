package main

import (
	qfext ".."
	"bytes"
	"fmt"
)

func main() {
	// helper routines are available to let you size your quotient filter
	// correctly
	conf := qfext.DetermineSize(1000000000, 0)
	fmt.Printf("a billion entry bloom filter would be loaded at %f percent...\n",
		conf.ExpectedLoading(1000000000),
	)
	conf.ExplainIndent("  ")

	data := []string{
		"red", "yellow", "orange", "blue",
	}
	// optimize your quotient filter when you know ahead of time
	// how many entries it will hold.  Otherwise, just use New()
	config := qfext.DetermineSize(uint(len(data)), 0)
	qf := qfext.NewWithConfig(config)
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
