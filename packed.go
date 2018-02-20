package qf

type packed struct {
	bits  uint8
	space []uint64
}

func newPacked(bits uint8, size uint) *packed {
	return &packed{bits, make([]uint64, size)}
}

func (p *packed) set(ix uint64, val uint64) (oldval uint64) {
	oldval, p.space[ix] = p.space[ix], val
	return
}

func (p *packed) get(ix uint64) (val uint64) {
	return p.space[ix]
}

func (p *packed) each(cb func(ix uint64, val uint64)) {
	for i, r := range p.space {
		cb(uint64(i), r)
	}
}

func (p *packed) size() uint {
	return uint(len(p.space))
}
