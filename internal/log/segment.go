package log

import (
	"EchoLog/api/v1"
	"fmt"
	"github.com/golang/protobuf/proto"
	"os"
	"path"
)

func newFileSegment(dirName string, startOffset uint64, config Config) (segment *fileSegment, err error) {
	segment = &fileSegment{
		startOffset: startOffset,
		config:      config,
	}

	storeFile, err := os.OpenFile(path.Join(dirName, fmt.Sprintf("%d.store", startOffset)),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}

	if segment.store, err = newFileStore(storeFile); err != nil {
		return nil, err
	}

	indexFile, err := os.OpenFile(path.Join(dirName, fmt.Sprintf("%d.index", startOffset)),
		os.O_RDWR|os.O_CREATE,
		0644,
	)

	if err != nil {
		return nil, err
	}

	if segment.index, err = newFileIndex(indexFile, config.MaxIndexBytes); err != nil {
		return nil, err
	}

	if off, _, err := segment.index.Read(-1); err != nil {
		segment.nextOffset = startOffset
	} else {
		segment.nextOffset = startOffset + uint64(off) + 1
	}

	return segment, nil
}

type fileSegment struct {
	store       *fileStore
	index       *fileIndex
	nextOffset  uint64
	startOffset uint64
	config      Config
}

func (fs *fileSegment) Append(rec *api.LogRecord) (appendIndex uint64, err error) {
	rec.Offset = fs.nextOffset
	appendIndex = fs.nextOffset

	data, err := proto.Marshal(rec)
	if err != nil {
		return 0, err
	}

	_, pos, err := fs.store.Append(data)
	if err != nil {
		return 0, err
	}

	err = fs.index.Write(
		uint32(fs.nextOffset-fs.startOffset),
		pos,
	)

	if err != nil {
		return 0, err
	}

	appendIndex = fs.nextOffset
	fs.nextOffset += 1
	return
}

func (fs *fileSegment) Read(offset uint64) (*api.LogRecord, error) {
	_, pos, err := fs.index.Read(int32(offset - fs.startOffset))
	if err != nil {
		return nil, err
	}

	data, err := fs.store.Read(pos)
	if err != nil {
		return nil, err
	}

	rec := &api.LogRecord{}
	if err = proto.Unmarshal(data, rec); err != nil {
		return nil, err
	}

	return rec, nil
}

func (fs *fileSegment) IsFull() bool {
	return fs.store.size >= fs.config.MaxStoreBytes || fs.index.size >= fs.config.MaxIndexBytes
}

func (fs *fileSegment) Close() error {
	if err := fs.store.Close(); err != nil {
		return err
	}

	if err := fs.index.Close(); err != nil {
		return err
	}

	return nil
}

func (fs *fileSegment) Remove() error {
	if err := fs.Close(); err != nil {
		return err
	}

	_ = os.Remove(fs.store.Name())
	_ = os.Remove(fs.index.Name())

	return nil
}
