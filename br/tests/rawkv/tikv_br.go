package main

import (
	"fmt"
	"strings"
	"time"
)

const (
	BrTimeout = time.Duration(30) * time.Minute
)

type TiKVBrRunCmd struct {
	command string
	options []string

	local bool
}

func NewTiKVBrCmd(command string) *TiKVBrRunCmd {
	b := &TiKVBrRunCmd{command: command}
	return b
}

func (b *TiKVBrRunCmd) Pd(pd string) *TiKVBrRunCmd {
	b.options = append(b.options, fmt.Sprintf("--pd=%s", pd))
	return b
}

func (b *TiKVBrRunCmd) SplitRegionMaxKeys(maxKeys uint) *TiKVBrRunCmd {
	b.options = append(b.options, fmt.Sprintf("--split-region-max-keys=%d", maxKeys))
	return b
}

func (b *TiKVBrRunCmd) GRPCMaxRecvMsgSize(size uint) *TiKVBrRunCmd {
	b.options = append(b.options, fmt.Sprintf("--grpc-max-recv-msg-size=%d", size))
	return b
}

func (b *TiKVBrRunCmd) Storage(storage string, isLocal bool) *TiKVBrRunCmd {
	b.options = append(b.options, fmt.Sprintf("--storage=%s", storage))
	b.local = isLocal
	return b
}

func (b *TiKVBrRunCmd) S3endpoint(s3endpoint string) *TiKVBrRunCmd {
	b.options = append(b.options, fmt.Sprintf("--s3.endpoint=%s", s3endpoint))
	return b
}

func (b *TiKVBrRunCmd) LogFile(logFile string) *TiKVBrRunCmd {
	b.options = append(b.options, fmt.Sprintf("--log-file=%s", logFile))
	return b
}

func (b *TiKVBrRunCmd) DstApiVersion(dstApiVersion string) *TiKVBrRunCmd {
	b.options = append(b.options, fmt.Sprintf("--dst-api-version=%s", dstApiVersion))
	return b
}

// safeInterval is seconds.
func (b *TiKVBrRunCmd) SafeInterval(safeInterval int64) *TiKVBrRunCmd {
	b.options = append(b.options, fmt.Sprintf("--safe-interval=%s", time.Duration(safeInterval)*time.Second))
	return b
}

func (b *TiKVBrRunCmd) Checksum(check bool) *TiKVBrRunCmd {
	checkStr := "false"
	if check {
		checkStr = "true"
	}
	b.options = append(b.options, fmt.Sprintf("--checksum=%s", checkStr))
	return b
}

func (b *TiKVBrRunCmd) CheckReq(checkReq bool) *TiKVBrRunCmd {
	b.options = append(b.options, fmt.Sprintf("--check-requirements=%t", checkReq))
	return b
}

func (b *TiKVBrRunCmd) StartKey(start []byte) *TiKVBrRunCmd {
	b.options = append(b.options, fmt.Sprintf("--start=%s", string(start)))
	return b
}

func (b *TiKVBrRunCmd) EndKey(end []byte) *TiKVBrRunCmd {
	b.options = append(b.options, fmt.Sprintf("--end=%s", string(end)))
	return b
}

func (b *TiKVBrRunCmd) Format(format string) *TiKVBrRunCmd {
	b.options = append(b.options, fmt.Sprintf("--format=%s", format))
	return b
}

func (b *TiKVBrRunCmd) CA(ca string) *TiKVBrRunCmd {
	b.options = append(b.options, fmt.Sprintf("--ca=%s", ca))
	return b
}

func (b *TiKVBrRunCmd) Cert(cert string) *TiKVBrRunCmd {
	b.options = append(b.options, fmt.Sprintf("--cert=%s", cert))
	return b
}

func (b *TiKVBrRunCmd) Key(key string) *TiKVBrRunCmd {
	b.options = append(b.options, fmt.Sprintf("--key=%s", key))
	return b
}

func (t *TiKVBrRunCmd) Command(command string) *TiKVBrRunCmd {
	t.command = command
	return t
}

func (s *TiKVBrRunCmd) Build() string {
	return fmt.Sprintf("%s %s", s.command, strings.Join(s.options[:], " "))
}
