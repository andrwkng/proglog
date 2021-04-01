package log

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	f, err := ioutil.TempFile(os.TempDir(), "index_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	c := Config{}
	c.Segment.MaxIndexBytes = 1024
	i, err := newIndex(f, c)
	require.NoError(t, err)

	_, _, err = i.Read(-1)
	require.Error(t, err)
	require.Equal(t, f.Name(), i.Name())

	entries := []struct {
		Off uint32
		Pos uint64
	}{
		{Off: 0, Pos: 0},
		{Off: 1, Pos: 10},
	}

	for _, want := range entries {
		err = i.Write(want.Off, want.Pos)
		require.NoError(t, err)
		_, pos, err := i.Read(int64(want.Off))
		require.NoError(t, err)
		require.Equal(t, want.Pos, pos)
	}
	// index and scanner should error when reading past existing entries
	_, _, err = i.Read(int64(len(entries)))
	require.Equal(t, io.EOF, err)
	_ = i.Close()
	// index should build its state from the existing file
	f, _ = os.OpenFile(f.Name(), os.O_RDWR, 0600)
	i, err = newIndex(f, c)
	require.NoError(t, err)
	off, pos, err := i.Read(-1)
	require.NoError(t, err)
	require.Equal(t, uint32(1), off)
	require.Equal(t, entries[1].Pos, pos)

}
