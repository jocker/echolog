package log

import (
	"encoding/binary"
	"github.com/edsrzf/mmap-go"
	"io"
	"os"
)

var (
	offsetWidth uint64 = 4 // sizeof(uint32)
	posWidth    uint64 = 8 // sizeof(uint64)
	entryWidth         = offsetWidth + posWidth
)

func newFileIndex(f *os.File, maxByteSize uint64) (*fileIndex, error) {

	if fi, err := os.Stat(f.Name()); err != nil {
		return nil, err
	} else {
		idx := &fileIndex{
			file: f,
			size: uint64(fi.Size()),
		}

		if err = os.Truncate(f.Name(), int64(maxByteSize)); err != nil {
			return nil, err
		}

		fi, _ = os.Stat(f.Name())
		idx.mmap, err = mmap.Map(f, mmap.RDWR, 0)
		if err != nil {
			return nil, err
		}

		return idx, nil
	}

}

type fileIndex struct {
	file *os.File
	size uint64
	mmap mmap.MMap
}

func (fi *fileIndex) Name() string {
	return fi.file.Name()
}

func (fi *fileIndex) Close() error {
	if err := fi.mmap.Flush(); err != nil {
		return err
	}

	if err := fi.file.Sync(); err != nil {
		return err
	}

	if err := fi.file.Truncate(int64(fi.size)); err != nil {
		return err
	}

	return fi.file.Close()
}

func (fi *fileIndex) Read(entryIndex int32) (outIndex uint32, pos uint64, err error) {
	if fi.size == 0 {
		err = io.EOF
		return
	}

	if entryIndex < 0 {
		entryIndex = int32(fi.size/entryWidth - 1) // last entry
	}

	entryOffset := uint64(entryIndex) * entryWidth

	outIndex = binary.BigEndian.Uint32(fi.mmap[entryOffset : entryOffset+offsetWidth])
	pos = binary.BigEndian.Uint64(fi.mmap[entryOffset+offsetWidth : entryOffset+entryWidth])
	return
}

func (fi *fileIndex) Write(offset uint32, pos uint64) error {
	if uint64(len(fi.mmap)) < fi.size+entryWidth {
		return io.EOF
	}
	binary.BigEndian.PutUint32(fi.mmap[fi.size:fi.size+offsetWidth], offset)
	binary.BigEndian.PutUint64(fi.mmap[fi.size+offsetWidth:fi.size+entryWidth], pos)

	fi.size += entryWidth

	return nil
}
