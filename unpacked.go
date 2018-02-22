package qf

import (
	"encoding/gob"
	"fmt"
	"io"
)

type unpacked []uint

var _ Vector = (*unpacked)(nil)

func UnpackedVectorAllocate(bits uint, size uint) Vector {
	if bits > WordSize {
		panic(fmt.Sprintf("bit size of %d is greater than word size of %s, not supported",
			bits, WordSize))
	}
	return make(unpacked, size)
}

func (v unpacked) Set(ix uint, val uint) {
	v[ix] = val
}

func (v unpacked) Swap(ix uint, val uint) (oldval uint) {
	v[ix], oldval = val, v[ix]
	return
}

func (v unpacked) Get(ix uint) (val uint) {
	return v[ix]
}

func (v unpacked) WriteTo(w io.Writer) (n int64, err error) {
	enc := gob.NewEncoder(w)
	err = enc.Encode(v)
	return int64(len(v)), err
}

func (v unpacked) Read(r io.Reader) (n Vector, err error) {
	var nv unpacked
	enc := gob.NewDecoder(r)
	err = enc.Decode(&nv)
	return nv, err
}
