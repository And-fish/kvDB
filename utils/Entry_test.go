package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValueStruct(t *testing.T) {
	v := ValueStruct{
		Value: []byte("熊玺皓"),
		Meta:  2,
		TTL:   213123123123,
	}
	data := make([]byte, v.ValEncodedSize())
	v.ValEncoding(data)
	var vv ValueStruct
	vv.ValDecode(data)
	assert.Equal(t, vv, v)
}
