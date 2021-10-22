package log

import (
	"EchoLog/api/v1"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func TestSegment(t *testing.T) {
	dir, _ := ioutil.TempDir(os.TempDir(), "temp_segment")
	defer os.RemoveAll(dir)

	rec := &api.LogRecord{
		Value: []byte("test-segment"),
	}

	entryCount := uint64(3)
	startOffset := uint64(32)

	segm, err := newFileSegment(dir, startOffset, Config{
		MaxStoreBytes: 1024,
		MaxIndexBytes: entryWidth * entryCount,
		InitialOffset: 0,
	})

	assert.NoError(t, err)
	assert.Equal(t, segm.startOffset, segm.nextOffset)
	assert.False(t, segm.IsFull())

	for i := uint64(0); i < entryCount; i++ {

		rec.Value = []byte(fmt.Sprintf("segment-%d", i))

		off, err := segm.Append(rec)
		assert.NoError(t, err)
		assert.Equal(t, startOffset+i, off)

		read, err := segm.Read(i)
		assert.NoError(t, err)
		assert.Equal(t, rec.Value, read.Value)
	}

	_, err = segm.Append(rec)
	assert.Equal(t, io.EOF, err)

	assert.True(t, segm.IsFull())

	assert.NoError(t, segm.Remove())
}
