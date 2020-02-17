package qf

// Reader is a readable quotient filter.  It is implmeneted by
// both Filter (raw backed r/w) and Disk (disk backed, ro)
type Reader interface {
	BitsOfStoragePerEntry() uint
	Len() uint64
	Contains([]byte) bool
	ContainsString(string) bool
	Lookup([]byte) (bool, uint64)
	LookupString(string) (bool, uint64)
}

var _ Reader = (*Disk)(nil)
var _ Reader = (*Filter)(nil)
