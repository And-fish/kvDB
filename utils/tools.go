package utils

func Copy(b []byte) []byte {
	buf := make([]byte, len(b))
	copy(buf, b)
	return b
}
