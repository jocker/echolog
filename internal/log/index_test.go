package log

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"testing"
)

func TestIndex(t *testing.T) {
	f, err := ioutil.TempFile(os.TempDir(), "index.tmp")
	assert.NoError(t, err)
	defer os.Remove(f.Name())

	idx, err := newFileIndex(f, 1024)
	assert.NoError(t, err)

	assert.Equal(t, f.Name(), idx.Name())

	_, _, err = idx.Read(-1)
	assert.Error(t, err)

	entries := []struct {
		Off uint32
		Pos uint64
	}{
		{Off: 0, Pos: 0},
		{Off: 1, Pos: 10},
	}

	for _, x := range entries {
		err = idx.Write(x.Off, x.Pos)
		assert.NoError(t, err)

		_, pos, err := idx.Read(int32(x.Off))
		assert.NoError(t, err)
		assert.Equal(t, x.Pos, pos)
	}

	_ = idx.Close()

	f, err = os.OpenFile(f.Name(), os.O_RDWR, 0600)
	assert.NoError(t, err)
	idx, err = newFileIndex(f, 1024)
	assert.NoError(t, err)

	for _, x := range entries {
		_, pos, err := idx.Read(int32(x.Off))
		assert.NoError(t, err)
		assert.Equal(t, x.Pos, pos)
	}

	x := entries[len(entries)-1]
	off, pos, err := idx.Read(-1)
	assert.NoError(t, err)
	assert.Equal(t, x.Off, off)
	assert.Equal(t, x.Pos, pos)
}
