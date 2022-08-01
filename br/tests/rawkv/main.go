package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	units "github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/rawkv"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var (
	maxMsgSize   = int(128 * units.MiB) // pd.ScanRegion may return a large response
	maxBatchSize = uint(1024)           // max batch size with BatchPut

	pdAddr        = flag.String("pd", "127.0.0.1:2379", "Address of PD")
	apiVersionInt = flag.Uint("api-version", 1, "Api version of tikv-server")
	br            = flag.String("br", "br", "The br binary to be tested.")
	brStorage     = flag.String("br-storage", "local:///tmp/backup_restore_test", "The url to store SST files of backup/resotre. Default: 'local:///tmp/backup_restore_test'")
)

type RawKVBRTester struct {
	pdAddr      string
	apiVersion  kvrpcpb.APIVersion
	br          string
	brStorage   string
	rawkvClient *rawkv.Client
	pdClient    pd.Client
}

func NewPDClient(ctx context.Context, pdAddrs string) (pd.Client, error) {
	addrs := strings.Split(pdAddrs, ",")
	maxCallMsgSize := []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSize)),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(maxMsgSize)),
	}
	return pd.NewClientWithContext(
		ctx, addrs, pd.SecurityOption{},
		pd.WithGRPCDialOptions(maxCallMsgSize...),
		pd.WithCustomTimeoutOption(10*time.Second),
		pd.WithMaxErrorRetry(3),
	)
}

func NewRawKVBRTester(ctx context.Context, pd, br, storage string, version kvrpcpb.APIVersion) (*RawKVBRTester, error) {
	cli, err := rawkv.NewClientWithOpts(context.TODO(), []string{pd},
		rawkv.WithAPIVersion(version))
	if err != nil {
		fmt.Println("fail to new rawkv client", err)
		return nil, err
	}
	pdClient, err := NewPDClient(ctx, pd)
	if err != nil {
		fmt.Println("fail to new pd client", err)
		return nil, err
	}
	return &RawKVBRTester{
		pdAddr:      pd,
		br:          br,
		apiVersion:  version,
		brStorage:   storage,
		rawkvClient: cli,
		pdClient:    pdClient,
	}, nil
}

func (t *RawKVBRTester) Close() {
	if t.rawkvClient != nil {
		t.rawkvClient.Close()
	}
	t.pdClient.Close()
}

func min(left, right uint) uint {
	if left < right {
		return left
	} else {
		return right
	}
}

func GenerateTestData(keyIndex uint, prefix []byte) (key, value []byte) {
	key = []byte(fmt.Sprintf("%s:_%019d", string(prefix), keyIndex))
	value = []byte(fmt.Sprintf("v0%020d", keyIndex))
	return key, value
}

func BatchGenerateData(keyIndex uint, keyCnt uint, prefix []byte) (keys, values [][]byte) {
	keys = make([][]byte, 0, keyCnt)
	values = make([][]byte, 0, keyCnt)
	for idx := keyIndex; idx < keyIndex+keyCnt; idx++ {
		key, value := GenerateTestData(idx, prefix)
		keys = append(keys, key)
		values = append(values, value)
	}
	return keys, values
}

// TODO: make it concurrency for large scale data.
func (t *RawKVBRTester) PreloadData(ctx context.Context, keyCnt uint, prefix []byte) error {
	startIdx := uint(0)
	for startIdx < keyCnt {
		batchCnt := min(maxBatchSize, keyCnt-startIdx)
		keys, values := BatchGenerateData(startIdx, batchCnt, prefix)
		err := t.rawkvClient.BatchPut(ctx, keys, values)
		if err != nil {
			return errors.Trace(err)
		}
		startIdx += batchCnt
	}
	return nil
}

func (t *RawKVBRTester) CleanData(ctx context.Context, prefix []byte) error {
	return t.rawkvClient.DeleteRange(ctx, prefix, append(prefix, 0))
}

func (t *RawKVBRTester) Checksum(ctx context.Context) (rawkv.RawChecksum, error) {
	return t.rawkvClient.Checksum(ctx, []byte{}, []byte{})
}

func (t *RawKVBRTester) Backup(ctx context.Context, dstAPIVersion kvrpcpb.APIVersion, safeInterval int64) ([]byte, error) {
	brCmd := NewTiKVBrCmd("backup raw")
	brCmdStr := brCmd.Pd(t.pdAddr).
		Storage(t.brStorage, true).
		CheckReq(false).
		DstApiVersion(dstAPIVersion.String()).
		SafeInterval(safeInterval).
		Checksum(true).
		Build()
	return t.ExecBRCmd(ctx, brCmdStr)
}

func (t *RawKVBRTester) Restore(ctx context.Context) ([]byte, error) {
	brCmd := NewTiKVBrCmd("restore raw")
	brCmdStr := brCmd.Pd(t.pdAddr).
		Storage(t.brStorage, true).
		CheckReq(false).
		Checksum(true).
		Build()
	return t.ExecBRCmd(ctx, brCmdStr)
}

func (t *RawKVBRTester) InjectFailpoint(failpoint string) error {
	return os.Setenv("GO_FAILPOINTS", failpoint)
}

func (t *RawKVBRTester) ExecBRCmd(ctx context.Context, cmdStr string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, t.br, strings.Split(cmdStr, " ")...)
	return cmd.Output()
}

func (t *RawKVBRTester) ClearStorage() error {
	return os.RemoveAll(t.brStorage)
}

func (t *RawKVBRTester) GetTso(ctx context.Context) (uint64, error) {
	p, l, err := t.pdClient.GetTS(ctx)
	if err != nil {
		return 0, err
	}
	return oracle.ComposeTS(p, l), nil
}

func ParseBackupTSFromOutput(output []byte) uint64 {
	flysnowRegexp := regexp.MustCompile(`BackupTS=([0-9]*)]`)
	if flysnowRegexp == nil {
		log.Panic("regex error")
	}
	params := flysnowRegexp.FindStringSubmatch(string(output))

	log.Info("backup output", zap.ByteString("output", output), zap.Int("match len", len(params)))
	for _, param := range params {
		log.Info("regex output", zap.String("regex output", param))
	}
	backupTs, err := strconv.ParseUint(params[len(params)-1], 10, 64)
	if err != nil {
		log.Panic("parse backup ts fails", zap.String("ts", params[len(params)-1]))
	}
	log.Info("get backup ts", zap.Uint64("ts", backupTs))
	return backupTs
}

func CheckBackupTS(apiVersion kvrpcpb.APIVersion, tso uint64, backupOutput []byte, safeInterval int64) {
	if apiVersion != kvrpcpb.APIVersion_V2 {
		return
	}
	backupTS := ParseBackupTSFromOutput(backupOutput)
	tsoPhysical := oracle.ExtractPhysical(tso)
	backupTSPhysical := oracle.ExtractPhysical(backupTS)
	diff := (tsoPhysical - backupTSPhysical) / 1000
	if math.Abs(float64(diff-safeInterval)) > 1.0 {
		log.Panic("backup ts does not match the rule", zap.Int64("backupTSPhysical", backupTSPhysical),
			zap.Int64("tsoPhysical", tsoPhysical), zap.Int64("safeInterval", safeInterval))
	}
}

func runTestWithFailPoint(failpoint string) error {
	apiVersion := kvrpcpb.APIVersion_V1TTL
	if *apiVersionInt == 2 {
		apiVersion = kvrpcpb.APIVersion_V2
	}
	ctx := context.TODO()

	fmt.Println("test api version", apiVersion)

	tester, err := NewRawKVBRTester(ctx, *pdAddr, *br, *brStorage, apiVersion)
	if err != nil {
		return errors.Annotate(err, "New Tester Fail")
	}
	err = tester.InjectFailpoint(failpoint)
	if err != nil {
		return errors.Annotatef(err, "Inject failpoint % s fail", failpoint)
	}

	if err := tester.ClearStorage(); err != nil {
		return errors.Annotate(err, "ClearStorage fail")
	}

	prefix := []byte("index")
	keyCnt := uint(10000)
	err = tester.PreloadData(ctx, keyCnt, prefix)
	if err != nil {
		return errors.Annotate(err, "Preload data fail")
	}

	oriChecksum, err := tester.Checksum(ctx)
	if err != nil {
		return errors.Annotate(err, "Preload checksum fail")
	}

	tso := uint64(0)
	if apiVersion == kvrpcpb.APIVersion_V2 {
		tso, err = tester.GetTso(ctx)
		if err != nil {
			return errors.Annotate(err, "Get tso fail")
		}
	}
	safeInterval := int64(120) // 2m
	backupOutput, err := tester.Backup(ctx, apiVersion, safeInterval)
	if err != nil {
		return errors.Annotatef(err, "Backup fail:%s", backupOutput)
	}
	log.Info("backup finish:", zap.ByteString("output", backupOutput))
	CheckBackupTS(apiVersion, tso, backupOutput, safeInterval)

	err = tester.CleanData(ctx, prefix)
	if err != nil {
		return errors.Annotate(err, "Clean data fail")
	}
	restoreOutput, err := tester.Restore(ctx)
	if err != nil {
		return errors.Annotatef(err, "Restore fail: %s", restoreOutput)
	}
	log.Info("Restore finish:", zap.ByteString("output", restoreOutput))
	restoreChecksum, err := tester.Checksum(ctx)
	if err != nil {
		return errors.Annotate(err, "Checksum fail")
	}
	if oriChecksum != restoreChecksum {
		return errors.Annotatef(err, "Checksum mismatch, src:%v, dst:%v", oriChecksum, restoreChecksum)
	}

	if apiVersion == kvrpcpb.APIVersion_V1TTL {
		if err := tester.ClearStorage(); err != nil {
			return errors.Annotate(err, "ClearStorage fail")
		}
		backupOutput, err := tester.Backup(ctx, kvrpcpb.APIVersion_V2, safeInterval)
		if err != nil {
			return errors.Annotatef(err, "Backup fail:%s", backupOutput)
		}
		log.Info("backup conversion finish:", zap.ByteString("output", backupOutput))
	}
	return nil
}

func main() {
	flag.Parse()
	failpoints := []string{"",
		"github.com/tikv/migration/br/pkg/backup/tikv-region-error=return(\"region error\")",
	}
	for _, failpoint := range failpoints {
		err := runTestWithFailPoint(failpoint)
		if err != nil {
			log.Error("run test with failpoint fails", zap.Error(err))
			os.Exit(1)
		}
	}
}
