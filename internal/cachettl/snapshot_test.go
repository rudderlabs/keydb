package cachettl

import (
	"bytes"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSnapshots(t *testing.T) {
	t.Run("empty cache", func(t *testing.T) {
		c := New[string, bool]()

		buf := bytes.NewBuffer(nil)
		err := CreateSnapshot(buf, c)
		require.NoError(t, err)
		require.Equal(t, 0, buf.Len())

		nc := New[string, bool]()
		require.NoError(t, LoadSnapshot(buf, nc))
		require.Equal(t, 0, nc.Len())
	})

	t.Run("non expired entries", func(t *testing.T) {
		now := time.Now()
		c := New[string, bool](WithNow(func() time.Time { return now }))
		c.Put("one", true, 1*time.Minute)
		c.Put("two", true, 2*time.Minute)
		c.Put("three", true, 3*time.Minute)

		buf := bytes.NewBuffer(nil)
		err := CreateSnapshot(buf, c)
		require.NoError(t, err)
		lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
		require.Equal(t, 3, len(lines))
		require.Equal(t, "one:"+strconv.FormatInt(now.Add(1*time.Minute).UnixMilli(), 10), lines[0])
		require.Equal(t, "two:"+strconv.FormatInt(now.Add(2*time.Minute).UnixMilli(), 10), lines[1])
		require.Equal(t, "three:"+strconv.FormatInt(now.Add(3*time.Minute).UnixMilli(), 10), lines[2])

		nc := New[string, bool]()
		require.NoError(t, LoadSnapshot(buf, nc))
		require.Equal(t, 3, nc.Len())
		require.True(t, nc.Get("one"))
		require.True(t, nc.Get("two"))
		require.True(t, nc.Get("three"))
		require.Equal(t, now.Add(1*time.Minute).UnixMilli(), nc.m["one"].expiration.UnixMilli())
		require.Equal(t, now.Add(2*time.Minute).UnixMilli(), nc.m["two"].expiration.UnixMilli())
		require.Equal(t, now.Add(3*time.Minute).UnixMilli(), nc.m["three"].expiration.UnixMilli())
	})

	t.Run("expired entries", func(t *testing.T) {
		now := time.Now()
		ft := func() time.Time { return now }
		c := New[string, bool](WithNow(ft))
		c.Put("one", true, 1*time.Minute)
		c.Put("two", true, 2*time.Minute)
		c.Put("three", true, 3*time.Minute)

		now = now.Add(1*time.Minute + time.Second) // expire the 1st element

		buf := bytes.NewBuffer(nil)
		err := CreateSnapshot(buf, c)
		require.NoError(t, err)
		lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
		require.Equal(t, 2, len(lines))
		require.Equal(t, "two:"+strconv.FormatInt(now.Add(59*time.Second).UnixMilli(), 10), lines[0])
		require.Equal(t, "three:"+strconv.FormatInt(now.Add(1*time.Minute+59*time.Second).UnixMilli(), 10), lines[1])

		now = now.Add(1*time.Minute + time.Second) // expire the 2nd element, only "three" should remain

		nc := New[string, bool](WithNow(ft))
		require.NoError(t, LoadSnapshot(buf, nc))
		require.Equal(t, 1, nc.Len())
		require.True(t, nc.Get("three"))
		require.Equal(t, now.Add(58*time.Second).UnixMilli(), nc.m["three"].expiration.UnixMilli())
	})
}
