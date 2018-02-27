package qf

const (
	offset64 = uint(14695981039346656037)
	prime64  = uint(1099511628211)
)

func fnvhash(v []byte) uint {
	// inline fnv 64
	hv := offset64
	for _, c := range v {
		hv *= prime64
		hv ^= uint(c)
	}
	return hv
}

// Mixing constants
const (
	big_m = 0xc6a4a7935bd1e995
	big_r = 47
)

func murmurhash64(v []byte) uint {
	var off int
	var h, k uint64

	h = uint64(len(v)) * big_m

	for l := (len(v) - off); l >= 8; l -= 8 {
		k = uint64(v[off+0]) | uint64(v[off+1])<<8 | uint64(v[off+2])<<16 | uint64(v[off+3])<<24 |
			uint64(v[off+4])<<32 | uint64(v[off+5])<<40 | uint64(v[off+6])<<48 | uint64(v[off+7])<<56

		k *= big_m
		k ^= k >> big_r
		k *= big_m

		h ^= k
		h *= big_m

		off += 8
	}

	switch len(v) - off {
	case 7:
		h ^= uint64(v[off+6]) << 48
		fallthrough
	case 6:
		h ^= uint64(v[off+5]) << 40
		fallthrough
	case 5:
		h ^= uint64(v[off+4]) << 32
		fallthrough
	case 4:
		h ^= uint64(v[off+3]) << 24
		fallthrough
	case 3:
		h ^= uint64(v[off+2]) << 16
		fallthrough
	case 2:
		h ^= uint64(v[off+1]) << 8
		fallthrough
	case 1:
		h ^= uint64(v[off+0])
		h *= big_m
	}

	h ^= h >> big_r
	h *= big_m
	h ^= h >> big_r

	return uint(h)
}
