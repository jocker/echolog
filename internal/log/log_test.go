package log

import (
	"EchoLog/api/v1"
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"path"
	"testing"
	"time"
)

func TestLog(t *testing.T) {
	dirPath := path.Join(os.TempDir(), "log-test", fmt.Sprintf("log-test-%d", time.Now().UnixNano()))
	defer os.Remove(dirPath)
	log, err := NewLog(dirPath, Config{})
	assert.NoError(t, err)
	testAppendRead(t, log)
}

func testAppendRead(t *testing.T, log *Log) {

	for i := 0; i <= 4; i++ {
		data := []byte(fmt.Sprintf("test log record %d", i))
		pos, err := log.Append(&api.LogRecord{
			Value: data,
		})
		assert.NoError(t, err)
		assert.Equal(t, uint64(i), pos)

		rec, err := log.Read(uint64(i))

		assert.NoError(t, err)

		assert.Equal(t, rec.Value, data)

	}

	rec, err := log.Read(5)
	assert.Error(t, err)
	assert.Nil(t, rec)

	low, high := log.Offsets()
	assert.Equal(t, low, uint64(0))
	assert.Equal(t, high, uint64(4))

}
