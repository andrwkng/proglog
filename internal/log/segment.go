package log

import (
	"fmt"
	"os"
	"path"

	api "github.com/andrwkng/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

// segment ties store and index together
type segment struct {
	store                  *store
	index                  *index
	baseOffset, nextOffset uint64
	config                 Config
}

// newSegment is called by the log when it needs to add a new segment
func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}
	var err error

	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}

	s.store, err = newStore(storeFile)
	if err != nil {
		return nil, err
	}

	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, err
	}

	s.index, err = newIndex(indexFile, c)
	if err != nil {
		return nil, err
	}

	off, _, err := s.index.Read(-1)
	// set the segment’s next offset to prepare for the next appended record
	if err != nil { // index is empty
		// the next record appended to the segment would be the first record
		// and its offset would be the segment’s base offset
		s.nextOffset = baseOffset
	} else { // index has at least one entry
		// the offset of the next record written should take the offset at the end of the segment,
		// which we get by adding 1 to the base offset and relative offset
		s.nextOffset = baseOffset + uint64(off) + 1
	}
	return s, nil
}

// Append writes the record to the segment and returns the newly appended
// record’s offset.
func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	cur := s.nextOffset
	record.Offset = cur
	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}

	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}

	err = s.index.Write(
		// index offsets are relative to base offset
		uint32(s.nextOffset-uint64(s.baseOffset)),
		pos,
	)
	if err != nil {
		return 0, err
	}

	s.nextOffset++
	return cur, nil
}

// Remove closes the segment and removes the index and store files.
func (s *segment) Remove() error {
	err := s.Close()
	if err != nil {
		return err
	}

	err = os.Remove(s.index.Name())
	if err != nil {
		return err
	}

	err = os.Remove(s.store.Name())
	if err != nil {
		return err
	}
	return nil
}

func (s *segment) Close() error {
	err := s.index.Close()
	if err != nil {
		return err
	}
	err = s.store.Close()
	if err != nil {
		return err
	}
	return nil
}

// Read returns the record for the given offset.
func (s *segment) Read(off uint64) (*api.Record, error) {
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}
	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}
	record := &api.Record{}
	err = proto.Unmarshal(p, record)
	return record, err
}

// IsMaxed returns whether the segment has reached its max size, either by
// writing too much to the store or the index.
func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes ||
		s.index.size >= s.config.Segment.MaxIndexBytes
}
