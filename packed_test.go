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
	for bits := uint8(1); bits <= 64; bits++ {
		n := uint(100)
		p := newPacked(bits, n)
		for j := 0; j < 100; j++ {
			for i := uint(0); i < n; i++ {
				v := uint64(r.Int63()) & ^p.forbiddenMask
				p.set(i, v)
				if !assert.Equal(t, v, p.get(i), "failed to write %s into %d", strconv.FormatUint(v, 2), i) {
					for i, x := range p.space {
						fmt.Printf("[%2d] %d) %s\n", j, i, strconv.FormatUint(x, 2))
					}
					return
				}
			}
		}
	}
}
