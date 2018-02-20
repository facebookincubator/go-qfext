package qf

type packed struct {
	bits  uint8
	space []uint64
}

func newPacked(bits uint8, size uint) *packed {
	return &packed{bits, make([]uint64, size)}
}

func (p *packed) set(ix uint, val uint64) (oldval uint64) {
	oldval, p.space[ix] = p.space[ix], val
	return
}

func (p *packed) get(ix uint) (val uint64) {
	return p.space[ix]
}

func (p *packed) len() uint {
	return uint(len(p.space))
}
