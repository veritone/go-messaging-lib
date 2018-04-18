package wfi

import (
	"bufio"
	"errors"
	"io"
	"net"
	"strings"
	"time"
)

// Ping it
func Ping(host string, duration time.Duration) error {
	// TODO: Support all types, not just tcp
	// "tcp",
	// "tcp4" (IPv4-only),
	// "tcp6" (IPv6-only),
	// "udp",
	// "udp4" (IPv4-only),
	// "udp6" (IPv6-only),
	// "ip",
	// "ip4" (IPv4-only),
	// "ip6" (IPv6-only),
	// "unix",
	// "unixgram"
	// "unixpacket"
	conn, err := net.DialTimeout("tcp", host, duration)
	if err == nil {
		return conn.Close()
	}
	return err
}

// Find parses from reader line by line to determine whether the given phrase exists.
// Currently there is no support for regex and the logic is built on top of `strings.Contains()` function
func Find(phrase string, r io.Reader, duration time.Duration) (string, error) {
	timer := time.NewTimer(duration)
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		select {
		case <-timer.C:
			return "", errors.New("cannot find phrase in time")
		default:
			line := scanner.Text()
			if strings.Contains(line, phrase) {
				return line, nil
			}
		}
	}
	return "", errors.New("phrase does not exist")
}
