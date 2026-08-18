package notifier

import (
	"crypto/rand"
	"encoding/hex"
	"time"
)

// newID returns a sortable, unique task id: a millisecond timestamp prefix (so
// ids roughly order by creation time and are easy to eyeball) followed by 8
// random bytes. Good enough for an internal service; no external UUID dep.
func newID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	ts := time.Now().UnixMilli()
	buf := make([]byte, 0, 13+16)
	buf = appendBase36(buf, ts)
	buf = append(buf, '_')
	buf = append(buf, hex.EncodeToString(b[:])...)
	return string(buf)
}

func appendBase36(b []byte, n int64) []byte {
	const digits = "0123456789abcdefghijklmnopqrstuvwxyz"
	if n == 0 {
		return append(b, '0')
	}
	var tmp [13]byte
	i := len(tmp)
	for n > 0 {
		i--
		tmp[i] = digits[n%36]
		n /= 36
	}
	return append(b, tmp[i:]...)
}
