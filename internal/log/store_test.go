package log

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"testing"
)

var (
	testWrites = []string{
		"text 1", "text 2", "text 3",
	}
)

func TestFileStore_Append(t *testing.T) {
	f, err := ioutil.TempFile(os.TempDir(), "file.tmp")
	assert.NoError(t, err)
	defer os.Remove(f.Name())

	fs, err := newFileStore(f)
	assert.NoError(t, err)

	do_fileStore_Append(t, fs)
	test_fileStore_Read(t, fs)
}

func do_fileStore_Append(t *testing.T, fs *fileStore) {
	var fsOffset uint64
	for _, txt := range testWrites {
		data := []byte(txt)

		byteSize, offset, err := fs.Append(data)
		assert.NoError(t, err)

		fsOffset += uint64(len(data) + recordLengthByteSize)

		assert.Equal(t, byteSize+offset, fsOffset)
	}

}

func test_fileStore_Read(t *testing.T, fs *fileStore) {
	var offset uint64

	for _, txt := range testWrites {
		readData, err := fs.Read(offset)
		assert.NoError(t, err)
		assert.Equal(t, txt, string(readData))
		offset += uint64(len(readData) + recordLengthByteSize)
	}

}
