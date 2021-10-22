package log

import (
	"EchoLog/api/v1"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type Log struct {
	mux           sync.Mutex
	dir           string
	config        Config
	segments      []*fileSegment
	activeSegment *fileSegment
}

func NewLog(dir string, config Config) (*Log, error) {

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err = os.MkdirAll(dir, os.ModePerm); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}

	if config.MaxIndexBytes == 0 {
		config.MaxIndexBytes = 1024
	}

	if config.MaxStoreBytes == 0 {
		config.MaxStoreBytes = 1024
	}

	log := &Log{
		dir:      dir,
		config:   config,
		segments: []*fileSegment{},
	}

	if err := log.setup(); err != nil {
		return nil, err
	}

	return log, nil
}

func (log *Log) setup() error {

	files, err := ioutil.ReadDir(log.dir)
	if err != nil {
		return err
	}

	processedNames := make(map[string]interface{})
	dummy := struct {
	}{}
	baseOffsets := make([]uint64, 0)
	for _, file := range files {
		baseName := strings.TrimSuffix(file.Name(), path.Ext(file.Name()))
		if _, ok := processedNames[baseName]; ok {
			continue
		}
		processedNames[baseName] = dummy
		offset, err := strconv.ParseUint(baseName, 10, 0)
		if err != nil {
			continue
		}

		baseOffsets = append(baseOffsets, offset)

	}

	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	for _, offset := range baseOffsets {
		if err = log.addSegmentForOffset(offset); err != nil {
			return err
		}
	}

	if len(log.segments) == 0 {
		if err = log.addSegmentForOffset(log.config.InitialOffset); err != nil {
			return err
		}
	}

	return nil
}

func (log *Log) addSegmentForOffset(offset uint64) error {
	if len(log.segments) > 0 {
		if offset != log.segments[len(log.segments)-1].nextOffset {
			return fmt.Errorf("invalid offset")
		}
	}
	segment, err := newFileSegment(log.dir, offset, log.config)
	if err != nil {
		return err
	}

	log.segments = append(log.segments, segment)
	log.activeSegment = segment
	return nil
}

// implements server.CommitLog.Append
func (log *Log) Append(rec *api.LogRecord) (appendIndex uint64, err error) {
	log.mux.Lock()
	defer log.mux.Unlock()
	appendIndex, err = log.activeSegment.Append(rec)
	if err != nil {
		return 0, err
	}

	if log.activeSegment.IsFull() {
		err = log.addSegmentForOffset(appendIndex + 1)
		if err != nil {
			return 0, err
		}
	}

	return appendIndex, nil
}

// implements server.CommitLog.Read
func (log *Log) Read(offset uint64) (*api.LogRecord, error) {
	log.mux.Lock()
	defer log.mux.Unlock()

	for _, segment := range log.segments {
		if segment.startOffset <= offset && segment.nextOffset > offset {
			return segment.Read(offset)
		}
	}

	return nil, api.ErrOffsetOutOfRange{Offset: offset}
}

func (log *Log) Close() error {
	return log.closeSegments(false)
}

func (log *Log) Remove() error {
	return log.closeSegments(true)
}

func (log *Log) closeSegments(removeSegments bool) error {
	log.mux.Lock()
	defer log.mux.Unlock()

	if len(log.segments) == 0 {
		return nil
	}
	truncateAt := -1
	for i, segment := range log.segments {
		var err error

		if removeSegments {
			err = segment.Remove()
		} else {
			err = segment.Close()
		}

		if err != nil {
			return err
		}
		truncateAt = i
		if log.activeSegment == segment {
			log.activeSegment = nil
		}
	}

	if truncateAt >= 0 {
		log.segments = log.segments[truncateAt:]
	}

	return nil
}

func (log *Log) Reset() error {
	if err := log.Remove(); err != nil {
		return err
	}
	return log.setup()
}

func (log *Log) Offsets() (low uint64, high uint64) {
	log.mux.Lock()
	defer log.mux.Unlock()

	if len(log.segments) > 0 {
		low = log.segments[0].startOffset
		high = log.segments[len(log.segments)-1].nextOffset
		if high > 0 {
			high -= 1
		}
	}
	return
}

func (log *Log) Truncate(lowest uint64) error {
	log.mux.Lock()
	defer log.mux.Unlock()

	for idx, segm := range log.segments {
		if segm.nextOffset <= lowest+1 {
			if segm == log.activeSegment {
				return fmt.Errorf("can't remove all segments")
			}
			if err := segm.Remove(); err != nil {
				return err
			}
			continue
		}
		log.segments = log.segments[idx:]
		break
	}

	return nil
}

func (log *Log) Reader() io.Reader {
	log.mux.Lock()
	defer log.mux.Unlock()

	readers := make([]io.Reader, len(log.segments))
	for idx, segment := range log.segments {
		readers[idx] = segment.store.Reader()
	}
	return io.MultiReader(readers...)
}
