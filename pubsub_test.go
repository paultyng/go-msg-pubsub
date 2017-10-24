package pubsub

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPSAtts(t *testing.T) {
	assert := assert.New(t)

	bad := map[string][]string{
		"foo": []string{"bar", "baz"},
	}

	actual, err := psAtts(bad)
	assert.Error(err)
	assert.Nil(actual)

	good := map[string][]string{
		"foo": nil,
		"bar": []string{"1"},
		"baz": []string{},
	}

	actual, err = psAtts(good)
	assert.NoError(err)
	assert.Equal(map[string]string{
		"foo": "",
		"bar": "1",
		"baz": "",
	}, actual)
}
