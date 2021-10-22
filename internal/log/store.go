package log

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"sync"
)

func newFileStore(f *os.File) (*fileStore, error) {
	if stat, err := os.Stat(f.Name()); err != nil {
		return nil, err
	} else {
		size := uint64(stat.Size())
		return &fileStore{
			File: f,
			buf:  bufio.NewWriter(f),
			size: size,
		}, nil
	}
}

// number of bytes used for storing the record length
// * because uint64 will always take 8 bytes
const recordLengthByteSize = 8

type fileStore struct {
	*os.File
	mux  sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func (fs *fileStore) Append(data []byte) (byteSize uint64, offset uint64, err error) {
	fs.mux.Lock()
	defer fs.mux.Unlock()

	recordLength := uint64(len(data))

	// write data byte size
	if err = binary.Write(fs.buf, binary.BigEndian, recordLength); err != nil {
		return
	}
	var nn int

	// write actual data
	if nn, err = fs.buf.Write(data); err != nil {
		return
	}

	byteSize = uint64(nn) + recordLengthByteSize
	offset = fs.size
	fs.size += byteSize
	return

}

func (fs *fileStore) Read(offset uint64) ([]byte, error) {
	fs.mux.Lock()
	defer fs.mux.Unlock()

	// flush the buffer before attempting to access the file
	if err := fs.buf.Flush(); err != nil {
		return nil, err
	}
	if _, err := fs.File.Seek(int64(offset), 0); err != nil {
		return nil, err
	}

	var dataSize uint64
	if err := binary.Read(fs.File, binary.BigEndian, &dataSize); err != nil {
		return nil, err
	}

	dataBytes := make([]byte, dataSize)

	if _, err := fs.File.Read(dataBytes); err != nil {
		return nil, err
	}

	return dataBytes, nil
}

func (fs *fileStore) ReadAt(p []byte, off int64) (n int, err error) {
	fs.mux.Lock()
	defer fs.mux.Unlock()

	if err = fs.buf.Flush(); err != nil {
		return
	}

	return fs.File.ReadAt(p, off)
}

func (fs *fileStore) Close() (err error) {
	fs.mux.Lock()
	defer fs.mux.Unlock()

	if err = fs.buf.Flush(); err != nil {
		return err
	}

	return fs.File.Close()
}

func (fs *fileStore) Reader() io.Reader {
	return &fileStoreReader{fs: fs, position: 0}
}

type fileStoreReader struct {
	fs       *fileStore
	position int64
}

func (reader *fileStoreReader) Read(p []byte) (n int, err error) {
	n, err = reader.fs.ReadAt(p, reader.position)
	reader.position += int64(n)
	return
}

var _ io.ReaderAt = (*fileStore)(nil)
var _ io.Closer = (*fileStore)(nil)
