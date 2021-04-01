package log

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var write = []byte("hello world")

func TestStoreAppendRead(t *testing.T) {
	file, err := ioutil.TempFile("", "store_append_read_test")
	require.NoError(t, err)
	defer os.Remove(file.Name())

	store, err := newStore(file)
	require.NoError(t, err)
	testAppend(t, store)
	testRead(t, store)
	testReadAt(t, store)

	store, err = newStore(file)
	require.NoError(t, err)
	testRead(t, store)

}

func testAppend(t *testing.T, s *store) {
	width := uint64(len(write)) + lenWidth
	n, pos, err := s.Append(write)
	require.NoError(t, err)
	require.Equal(t, pos+n, width)
}

func testRead(t *testing.T, s *store) {
	var pos uint64
	read, err := s.Read(pos)
	require.NoError(t, err)
	require.Equal(t, write, read)
}

func testReadAt(t *testing.T, s *store) {
	off := int64(0)
	b := make([]byte, lenWidth)
	n, err := s.ReadAt(b, off)
	require.NoError(t, err)
	require.Equal(t, lenWidth, n)
}

func TestStoreClose(t *testing.T) {
	f, err := ioutil.TempFile("", "store_close_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	s, err := newStore(f)
	require.NoError(t, err)

	_, _, err = s.Append(write)
	require.NoError(t, err)

	f, sizeBefore, err := openFile(f.Name())
	require.NoError(t, err)

	err = s.Close()
	require.NoError(t, err)

	f, sizeAfter, err := openFile(f.Name())
	require.NoError(t, err)

	require.True(t, sizeAfter > sizeBefore)
}

func openFile(name string) (file *os.File, size int64, err error) {
	f, err := os.OpenFile(
		name,
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, 0, err
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	return f, fi.Size(), nil
}
