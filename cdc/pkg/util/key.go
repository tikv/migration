package util

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"strings"

	"github.com/pingcap/errors"
)

var (
	// APIV2RawKeyPrefix is prefix of raw key in API V2.
	APIV2RawKeyPrefix = []byte{'r', 0, 0, 0}
	// APIV2RawEndKey is max key of raw key in API V2.
	APIV2RawEndKey = []byte{'r', 0, 0, 1}
)

// EncodeV2Key encode a user key into API V2 format.
func EncodeV2Key(key []byte) []byte {
	return append(APIV2RawKeyPrefix, key...)
}

// EncodeV2Range encode a range into API V2 format.
// TODO: resue code in client-go
func EncodeV2Range(start, end []byte) ([]byte, []byte) {
	var b []byte
	if len(end) > 0 {
		b = EncodeV2Key(end)
	} else {
		b = APIV2RawEndKey
	}
	return EncodeV2Key(start), b
}

// EncodeKeySpan parse user input key and encode them to apiv2 format.
func EncodeKeySpan(format, start, end string) ([]byte, []byte, error) {
	startKey, err := ParseKey(format, start)
	if err != nil {
		return nil, nil, err
	}
	endKey, err := ParseKey(format, end)
	if err != nil {
		return nil, nil, err
	}
	encodedStart, encodedEnd := EncodeV2Range(startKey, endKey)
	return encodedStart, encodedEnd, nil
}

func ValidKeyFormat(format, start, end string) error {
	_, _, err := EncodeKeySpan(format, start, end)
	return errors.Annotatef(err, "Invalid key format, start:%s, end:%s, format:%s. Err", start, end, format)
}

// ParseKey parse key by given format.
// TODO: same code with br/pkg/utils/key.go, need make them common.
func ParseKey(format, key string) ([]byte, error) {
	switch format {
	case "raw":
		return []byte(key), nil
	case "escaped":
		return unescapedKey(key)
	case "hex":
		key, err := hex.DecodeString(key)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return key, nil
	}
	return nil, errors.Errorf("inavlid key format:%s", format)
}

// Ref PD: https://github.com/pingcap/pd/blob/master/tools/pd-ctl/pdctl/command/region_command.go#L334
func unescapedKey(text string) ([]byte, error) {
	var buf []byte
	r := bytes.NewBuffer([]byte(text))
	for {
		c, err := r.ReadByte()
		if err != nil {
			if errors.Cause(err) != io.EOF { // nolint:errorlint
				return nil, errors.Trace(err)
			}
			break
		}
		if c != '\\' {
			buf = append(buf, c)
			continue
		}
		n := r.Next(1)
		if len(n) == 0 {
			return nil, io.EOF
		}
		// See: https://golang.org/ref/spec#Rune_literals
		if idx := strings.IndexByte(`abfnrtv\'"`, n[0]); idx != -1 {
			buf = append(buf, []byte("\a\b\f\n\r\t\v\\'\"")[idx])
			continue
		}

		switch n[0] {
		case 'x':
			fmt.Sscanf(string(r.Next(2)), "%02x", &c)
			buf = append(buf, c)
		default:
			n = append(n, r.Next(2)...)
			_, err := fmt.Sscanf(string(n), "%03o", &c)
			if err != nil {
				return nil, errors.Trace(err)
			}
			buf = append(buf, c)
		}
	}
	return buf, nil
}
