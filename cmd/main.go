// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved

package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	qf "github.com/facebookincubator/go-qfext"

	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Commands: []*cli.Command{
			{
				Name:  "compile",
				Usage: "compile a list of terms into a quotient filter",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "output",
						Aliases: []string{"out", "o"},
						Value:   "qf.bin",
						Usage:   "name of the file to write the quotient filter to",
					},
					&cli.StringFlag{
						Name:    "input",
						Aliases: []string{"in", "i"},
						Usage:   "file to read from (default is stdin)",
					},
					&cli.BoolFlag{
						Name:    "bitpacked",
						Aliases: []string{"p"},
						Usage:   "whether to bitpack the output",
					},
				},
				Action: func(c *cli.Context) error {
					output := c.String("output")
					if _, err := os.Stat(output); !os.IsNotExist(err) {
						return fmt.Errorf("refusing to over-write existing file: %s", output)
					}
					if c.NArg() > 0 {
						return fmt.Errorf("unexpected command line arguments: %q", c.Args().Slice())
					}

					var reader io.Reader
					if c.IsSet("input") {
						f, err := os.Open(c.String("input"))
						if err != nil {
							return err
						}
						reader = f
						defer f.Close()
					} else {
						reader = os.Stdin
					}

					filter := qf.NewWithConfig(qf.Config{BitPacked: c.Bool("bitpacked")})
					rdr := bufio.NewReader(reader)
					start := time.Now()
					for {
						l, _, err := rdr.ReadLine()
						if err != nil {
							if err == io.EOF {
								break
							}
							return err
						}
						s := strings.TrimSpace(string(l))
						filter.InsertString(s)
					}
					log.Printf("built in memory quotient filter in %s", time.Since(start))
					o, e := os.Create(output)
					if e != nil {
						return fmt.Errorf("error opening %s: %s", output, e)
					}
					defer o.Close()
					if n, err := filter.WriteTo(o); err != nil {
						return fmt.Errorf("error writing quotient filter: %s", err)
					} else {
						log.Printf("wrote %d bytes to %s", n, output)
					}
					filter.DebugDump(false)
					return nil
				},
			},
			{
				Name:  "lookup",
				Usage: "lookup a string from a quotient filter",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "input",
						Aliases: []string{"in", "i"},
						Usage:   "file containing quotient filter",
					},
				},
				Action: func(c *cli.Context) error {
					filter, err := qf.OpenReadOnlyFromPath(c.String("i"))
					if err != nil {
						return fmt.Errorf("lookup: can't read input file: %w", err)
					}
					test := strings.Join(c.Args().Slice(), " ")
					found, ext := filter.LookupString(test)
					fmt.Printf("lookup %q: %t", test, found)
					if filter.HasStorage() && found {
						fmt.Printf(" - value: %d", ext)
					}
					fmt.Printf("\n")
					return nil
				},
			},
			{
				Name:  "describe",
				Usage: "read the header from a quotient filter and describe it",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "input",
						Aliases: []string{"in", "i"},
						Usage:   "file containing quotient filter",
					},
				},
				Action: func(c *cli.Context) error {
					h, err := qf.ReadHeaderFromPath(c.String("i"))
					if err != nil {
						return fmt.Errorf("describe: can't read input file: %w", err)
					}
					fmt.Printf("Quotient filter version %d\n", h.Version)
					not := "not "
					if h.BitPacked {
						not = ""
					}
					fmt.Printf("%sbitpacked - %d entries, %d quotient bits, %d storage bits\n",
						not, h.Entries, h.QBits, h.StorageBits)
					return nil
				},
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
