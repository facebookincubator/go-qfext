package main

import (
	qfext ".."
	"bytes"
	"fmt"
)

func main() {
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
	qf.DebugDump()

	// Serialize the quotient filter and report size
	buf := bytes.NewBuffer([]byte{})
	qf.WriteTo(buf)
	fmt.Printf("QF serializes into %d bytes\n", buf.Len())
}
