package log

import (
	"fmt"
	"io/ioutil"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "github.com/andrwkng/proglog/api/v1"
)

// Log consisits of a list of segments
type Log struct {
	mu sync.RWMutex

	Dir    string // location where segments are stored
	Config Config

	activeSegment *segment // pointer to the active segment to append writes to`
	segments      []*segment
}

func NewLog(dir string, c Config) (*Log, error) {
	// set defaults for the configs
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}

	// create a log instance and setup the instance
	l := &Log{
		Dir:    dir,
		Config: c,
	}
	return l, l.setup()
}

// Append appends a record to the log. We append the record to the active segment.
// Afterward, if the segment is at its max size (per the max size configs),
// then we make a new active segment.
func (l *Log) Append(record *api.Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	off, err := l.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}

	if l.activeSegment.IsMaxed() {
		err = l.newSegment(off + 1)
	}
	return off, err

}

// Read reads the record stored at the given offset.
func (l *Log) Read(off uint64) (*api.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	var s *segment
	for _, segment := range l.segments {
		if segment.baseOffset <= off && off < segment.nextOffset {
			s = segment
			break
		}
	}
	if s == nil || s.nextOffset <= off {
		return nil, fmt.Errorf("offset out of range: %d", off)
	}
	return s.Read(off)
}

// Close iterates over the segments and closes them
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (l *Log) LowestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segments[0].baseOffset, nil
}

func (l *Log) HighestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	off := l.segments[len(l.segments)-1].nextOffset
	if off == 0 {
		return 0, nil
	}
	return off - 1, nil
}

// Truncate removes all segments whose highest offset is lower than
// lowest.
func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var segments []*segment
	for _, s := range l.segments {
		if s.nextOffset <= lowest+1 {
			if err := s.Remove(); err != nil {
				return err
			}
			continue
		}
		segments = append(segments, s)
	}
	l.segments = segments
	return nil
}

// setup is responsible for setting the log up for the segments that
// already exist on disk or, if the log is new and has no existing segments, for
// bootstrapping the initial segment
func (l *Log) setup() error {
	files, err := ioutil.ReadDir(l.Dir)
	if err != nil {
		return err
	}
	var baseOffsets []uint64
	for _, file := range files {
		offStr := strings.TrimSuffix(
			file.Name(),
			path.Ext(file.Name()),
		)
		off, _ := strconv.ParseUint(offStr, 10, 0)
		//
		baseOffsets = append(baseOffsets, off)
	}

	// sort the base offsets (because we want our slice of segments to be in order from oldest to newest)
	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	// create the segments
	for i := 0; i < len(baseOffsets); i++ {
		err := l.newSegment(baseOffsets[i])
		if err != nil {
			return err
		}
		// baseOffset contains dup for index and store so we skip the dup
		i++
	}
	//  if the log has no existing segments, bootstrap the initial segment
	if l.segments == nil {
		err := l.newSegment(l.Config.Segment.InitialOffset)
		if err != nil {
			return err
		}
	}
	return nil
}

// newSegment creates a new segment, appends that segment to the log’s
// slice of segments, and makes the new segment the active segment so that
// subsequent append calls write to it.
func (l *Log) newSegment(off uint64) error {
	s, err := newSegment(l.Dir, off, l.Config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	l.activeSegment = s
	return nil
}
