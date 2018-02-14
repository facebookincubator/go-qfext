// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved

package qf

import (
	"testing"

	"fmt"
	"math/rand"
	"strconv"

	"github.com/stretchr/testify/assert"
)

func TestBitPacking(t *testing.T) {
	r := rand.NewSource(77) //intentionally fixed seed
	for bits := uint(1); bits <= 64; bits++ {
		n := uint64(100)
		p := BitPackedVectorAllocate(bits, n).(*packed)
		for j := 0; j < 100; j++ {
			for i := uint64(0); i < n; i++ {
				v := uint64(r.Int63()) & ^p.forbiddenMask
				p.Set(i, v)
				if !assert.Equal(t, v, p.Get(i), "failed to write %s into %d", strconv.FormatUint(uint64(v), 2), i) {
					for i, x := range p.space {
						fmt.Printf("[%2d] %d) %s\n", j, i, strconv.FormatUint(uint64(x), 2))
					}
					return
				}
			}
		}
	}
}
