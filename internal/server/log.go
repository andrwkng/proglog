package server

import "fmt"

type Record struct {
	Value  []byte `json:"value"`
	Offset uint64 `json:"offset"`
}

type Log struct {
	records []Record
}

func NewLog() *Log {
	return &Log{}
}

func (log *Log) Append(record Record) (uint64, error) {
	record.Offset = uint64(len(log.records))
	log.records = append(log.records, record)
	return record.Offset, nil
}

func (log *Log) Read(offset uint64) (Record, error) {
	if offset >= uint64(len(log.records)) {
		return Record{}, ErrOffsetNotFound
	}
	return log.records[offset], nil
}

var ErrOffsetNotFound = fmt.Errorf("Offset not found")
