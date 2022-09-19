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
	"sync/atomic"
	"time"

	units "github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/rawkv"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

var (
	maxMsgSize   = int(128 * units.MiB) // pd.ScanRegion may return a large response
	maxBatchSize = uint(1024)           // max batch size with BatchPut

	keyCnt        = flag.Uint("keycnt", 3000000, "KeyCnt of testing")
	thread        = flag.Uint("thread", 500, "Thread of preloading data")
	pdAddr        = flag.String("pd", "127.0.0.1:2379", "Address of PD")
	apiVersionInt = flag.Uint("api-version", 1, "Api version of tikv-server")
	br            = flag.String("br", "br", "The br binary to be tested.")
	brStorage     = flag.String("br-storage", "local:///tmp/backup_restore_test", "The url to store SST files of backup/resotre.")
	coverageDir   = flag.String("coverage", "", "The coverage profile file dir of test.")
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

func (t *RawKVBRTester) PreloadData(ctx context.Context, keyCnt, thread uint, prefix []byte) error {
	errGroup := new(errgroup.Group)
	keyCntPerBatch := keyCnt / thread
	totalCnt := uint32(0)
	for i := uint(0); i < thread; i++ {
		startIdx := i * keyCntPerBatch
		endIdx := (i + 1) * keyCntPerBatch
		errGroup.Go(func() error {
			start := startIdx
			end := endIdx
			for start < end {
				batchCnt := min(maxBatchSize, end-start)
				keys, values := BatchGenerateData(start, batchCnt, prefix)
				err := t.rawkvClient.BatchPut(ctx, keys, values)
				if err != nil {
					return err
				}
				atomic.AddUint32(&totalCnt, uint32(batchCnt))
				start += batchCnt
			}
			return nil
		})
	}
	err := errGroup.Wait()
	log.Info("Preload finish", zap.Uint32("Total", totalCnt),
		zap.Uint("Thread", thread), zap.Uint("KeyCnt", keyCnt), zap.Error(err))
	return err
}

func (t *RawKVBRTester) CleanData(ctx context.Context, prefix []byte) error {
	start := prefix
	end := append(prefix, []byte{0xFF, 0xFF, 0xFF, 0xFF}...)
	err := t.rawkvClient.DeleteRange(ctx, start, end)
	if err != nil {
		return errors.Annotate(err, "Delete range fails.")
	}
	// scan to verify delete range result.
	keys, _, err := t.rawkvClient.Scan(ctx, start, end, 1024)
	if err != nil {
		return errors.Annotate(err, "Scan data fails.")
	}
	if len(keys) != 0 {
		return errors.Errorf("Not empty after clean data, len:%d.", len(keys))
	}
	log.Info("Keys with prefix is deleted.", zap.ByteString("Prefix", prefix))
	return nil
}

func (t *RawKVBRTester) Checksum(ctx context.Context, start, end []byte) (rawkv.RawChecksum, error) {
	return t.rawkvClient.Checksum(ctx, start, end)
}

func (t *RawKVBRTester) Backup(ctx context.Context, dstAPIVersion kvrpcpb.APIVersion, safeInterval int64,
	startKey, endKey []byte) ([]byte, error) {
	brCmd := NewTiKVBrCmd("backup raw")
	brCmdStr := brCmd.Pd(t.pdAddr).
		Storage(t.brStorage, true).
		CheckReq(false).
		DstApiVersion(dstAPIVersion.String()).
		SafeInterval(safeInterval).
		StartKey(startKey).
		EndKey(endKey).
		Format("raw").
		Checksum(true).
		Build()
	return t.ExecBRCmd(ctx, brCmdStr)
}

func (t *RawKVBRTester) Restore(ctx context.Context, startKey, endKey []byte) ([]byte, error) {
	brCmd := NewTiKVBrCmd("restore raw")
	brCmdStr := brCmd.Pd(t.pdAddr).
		Storage(t.brStorage, true).
		StartKey(startKey).
		EndKey(endKey).
		Format("raw").
		CheckReq(false).
		Checksum(true).
		Build()
	return t.ExecBRCmd(ctx, brCmdStr)
}

func (t *RawKVBRTester) InjectFailpoint(failpoint string) error {
	return os.Setenv("GO_FAILPOINTS", failpoint)
}

func (t *RawKVBRTester) ExecBRCmd(ctx context.Context, cmdStr string) ([]byte, error) {
	log.Info("exec br cmd", zap.String("br", t.br), zap.String("args", cmdStr))
	covFile, err := os.CreateTemp(*coverageDir, "cov.integration.*.out")
	if err != nil {
		return nil, err
	}
	defer covFile.Close()
	cmdParameter := []string{fmt.Sprintf("-test.coverprofile=%s", covFile.Name())}
	//cmdParameter := []string{}
	cmdParameter = append(cmdParameter, strings.Split(cmdStr, " ")...)
	cmd := exec.CommandContext(ctx, t.br, cmdParameter...)
	return cmd.Output()
}

func (t *RawKVBRTester) ClearStorage() error {
	err := os.RemoveAll(t.brStorage)
	log.Info("Backup storage is cleared", zap.String("Path", t.brStorage))
	return err
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

func runBackupAndRestore(ctx context.Context, tester *RawKVBRTester, prefix, start, end []byte) {
	oriChecksum, err := tester.Checksum(ctx, start, end)
	if err != nil {
		log.Panic("Preload checksum fail", zap.Error(err))
	}

	tso := uint64(0)
	if tester.apiVersion == kvrpcpb.APIVersion_V2 {
		tso, err = tester.GetTso(ctx)
		if err != nil {
			log.Panic("Get tso fail", zap.Error(err))
		}
	}
	safeInterval := int64(120) // 2m
	backupOutput, err := tester.Backup(ctx, tester.apiVersion, safeInterval, start, end)
	if err != nil {
		log.Panic("Backup fail", zap.Error(err), zap.ByteString("output", backupOutput))
	}
	log.Info("backup finish:", zap.ByteString("output", backupOutput))
	CheckBackupTS(tester.apiVersion, tso, backupOutput, safeInterval)

	err = tester.CleanData(ctx, prefix)
	if err != nil {
		log.Panic("Clean data fail", zap.Error(err))
	}
	restoreOutput, err := tester.Restore(ctx, start, end)
	if err != nil {
		log.Panic("Restore fail", zap.Error(err), zap.ByteString("restore output", restoreOutput))
	}
	log.Info("Restore finish:", zap.ByteString("output", restoreOutput))
	restoreChecksum, err := tester.Checksum(ctx, start, end)
	if err != nil {
		log.Panic("Checksum fail", zap.Error(err))
	}
	if oriChecksum != restoreChecksum {
		log.Panic("Checksum mismatch", zap.Reflect("src", oriChecksum), zap.Reflect("dst", restoreChecksum))
	}
	log.Info("Checksum pass")
}

func runTestWithFailPoint(failpoint string, prefix []byte, backupRange *kvrpcpb.KeyRange) {
	apiVersion := kvrpcpb.APIVersion_V1TTL
	if *apiVersionInt == 2 {
		apiVersion = kvrpcpb.APIVersion_V2
	}
	ctx := context.TODO()

	fmt.Println("test api version", apiVersion)

	tester, err := NewRawKVBRTester(ctx, *pdAddr, *br, *brStorage, apiVersion)
	if err != nil {
		log.Panic("New Tester Fail", zap.Error(err))
	}
	defer tester.Close()

	err = tester.InjectFailpoint(failpoint)
	if err != nil {
		log.Panic("Inject failpoint fail", zap.Error(err), zap.String("failpoint", failpoint))
	}

	if err := tester.ClearStorage(); err != nil {
		log.Panic("ClearStorage fail", zap.Error(err))
	}
	err = tester.CleanData(ctx, prefix)
	if err != nil {
		log.Panic("Clean data fail", zap.Error(err))
	}
	err = tester.PreloadData(ctx, *keyCnt, *thread, prefix)
	if err != nil {
		log.Panic("Preload data fail", zap.Error(err))
	}
	runBackupAndRestore(ctx, tester, prefix, backupRange.StartKey, backupRange.EndKey)

	if apiVersion == kvrpcpb.APIVersion_V1TTL {
		if err := tester.ClearStorage(); err != nil {
			log.Panic("ClearStorage fail", zap.Error(err))
		}
		safeInterval := int64(120) // 2m
		backupOutput, err := tester.Backup(ctx, kvrpcpb.APIVersion_V2, safeInterval, backupRange.StartKey, backupRange.EndKey)
		if err != nil {
			log.Panic("Backup fail", zap.Error(err), zap.ByteString("backup output", backupOutput))
		}
		log.Info("backup conversion finish:", zap.ByteString("output", backupOutput))
	}

}

func main() {
	flag.Parse()
	failpoints := []string{"",
		"github.com/tikv/migration/br/pkg/backup/tikv-region-error=return(\"region error\")",
	}
	prefix := []byte("index")
	q1Key, _ := GenerateTestData(*keyCnt/4, prefix)
	q3Key, _ := GenerateTestData(3**keyCnt/4, prefix)
	backupRanges := []kvrpcpb.KeyRange{
		{StartKey: []byte{}, EndKey: []byte{}},
		{StartKey: q1Key, EndKey: q3Key},
	}
	for _, failpoint := range failpoints {
		for _, backupRange := range backupRanges {
			runTestWithFailPoint(failpoint, prefix, &backupRange)
		}
	}
}
