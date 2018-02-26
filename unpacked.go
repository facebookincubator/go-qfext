package qf

import (
	"encoding/gob"
	"fmt"
	"io"
)

type unpacked []uint

var _ Vector = (*unpacked)(nil)

func UnpackedVectorAllocate(bits uint, size uint) Vector {
	if bits > BitsPerWord {
		panic(fmt.Sprintf("bit size of %d is greater than word size of %s, not supported",
			bits, BitsPerWord))
	}
	arr := make(unpacked, size)
	return &arr
}

func (v *unpacked) Set(ix uint, val uint) {
	(*v)[ix] = val
}

func (v *unpacked) Swap(ix uint, val uint) (oldval uint) {
	(*v)[ix], oldval = val, (*v)[ix]
	return
}

func (v *unpacked) Get(ix uint) (val uint) {
	return (*v)[ix]
}

func (v *unpacked) WriteTo(w io.Writer) (n int64, err error) {
	enc := gob.NewEncoder(w)
	err = enc.Encode(*v)
	return int64(len(*v) * BytesPerWord), err
}

func (v *unpacked) ReadFrom(r io.Reader) (n int64, err error) {
	enc := gob.NewDecoder(r)
	err = enc.Decode(v)
	return int64(len(*v) * BytesPerWord), err
}
