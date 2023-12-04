// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package backup_test

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
	. "github.com/pingcap/check" // nolint:revive
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	"github.com/tikv/migration/br/pkg/backup"
	"github.com/tikv/migration/br/pkg/conn"
	"github.com/tikv/migration/br/pkg/metautil"
	"github.com/tikv/migration/br/pkg/pdutil"
	"github.com/tikv/migration/br/pkg/storage"
	pd "github.com/tikv/pd/client"
)

type testBackup struct {
	ctx    context.Context
	cancel context.CancelFunc

	mockPDClient pd.Client
	backupClient *backup.Client

	storage storage.ExternalStorage
}

var _ = Suite(&testBackup{})

func TestT(t *testing.T) {
	TestingT(t)
}

func (r *testBackup) SetUpSuite(c *C) {
	_, mockCluster, pdClient, err := testutils.NewMockTiKV("", nil)
	c.Assert(err, IsNil)
	mockCluster.AddStore(0, "127.0.0.1")
	r.mockPDClient = pdClient
	r.ctx, r.cancel = context.WithCancel(context.Background())
	mockMgr := &conn.Mgr{PdController: &pdutil.PdController{}}
	mockMgr.SetPDClient(r.mockPDClient)
	mockMgr.SetHTTP([]string{"test"}, nil)
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	// Exact URL match
	httpmock.RegisterResponder("GET", `=~^/config`,
		httpmock.NewStringResponder(200, `{"storage":{"api-version":2, "enable-ttl":true}}`))

	r.backupClient, err = backup.NewBackupClient(r.ctx, mockMgr, nil)
	c.Assert(err, IsNil)

	base := c.MkDir()
	r.storage, err = storage.NewLocalStorage(base)
	c.Assert(err, IsNil)
}

func (r *testBackup) resetStorage(c *C) {
	var err error
	base := c.MkDir()
	r.storage, err = storage.NewLocalStorage(base)
	c.Assert(err, IsNil)
}

func (r *testBackup) TestGetTS(c *C) {
	var (
		err error
		// mockPDClient' physical ts and current ts will have deviation
		// so make this deviation tolerance 100ms
		deviation = 100
	)

	// timeago not work
	expectedDuration := 0
	currentTS := time.Now().UnixNano() / int64(time.Millisecond)
	ts, err := r.backupClient.GetTS(r.ctx, 0, 0)
	c.Assert(err, IsNil)
	pdTS := oracle.ExtractPhysical(ts)
	duration := int(currentTS - pdTS)
	c.Assert(duration, Greater, expectedDuration-deviation)
	c.Assert(duration, Less, expectedDuration+deviation)

	// timeago = "1.5m"
	expectedDuration = 90000
	currentTS = time.Now().UnixNano() / int64(time.Millisecond)
	ts, err = r.backupClient.GetTS(r.ctx, 90*time.Second, 0)
	c.Assert(err, IsNil)
	pdTS = oracle.ExtractPhysical(ts)
	duration = int(currentTS - pdTS)
	c.Assert(duration, Greater, expectedDuration-deviation)
	c.Assert(duration, Less, expectedDuration+deviation)

	// timeago = "-1m"
	_, err = r.backupClient.GetTS(r.ctx, -time.Minute, 0)
	c.Assert(err, ErrorMatches, "negative timeago is not allowed.*")

	// timeago = "1000000h" overflows
	_, err = r.backupClient.GetTS(r.ctx, 1000000*time.Hour, 0)
	c.Assert(err, ErrorMatches, ".*backup ts overflow.*")

	// timeago = "10h" exceed GCSafePoint
	p, l, err := r.mockPDClient.GetTS(r.ctx)
	c.Assert(err, IsNil)
	now := oracle.ComposeTS(p, l)
	_, err = r.mockPDClient.UpdateGCSafePoint(r.ctx, now)
	c.Assert(err, IsNil)
	_, err = r.backupClient.GetTS(r.ctx, 10*time.Hour, 0)
	c.Assert(err, ErrorMatches, ".*GC safepoint [0-9]+ exceed TS [0-9]+.*")

	// timeago and backupts both exists, use backupts
	backupts := oracle.ComposeTS(p+10, l)
	ts, err = r.backupClient.GetTS(r.ctx, time.Minute, backupts)
	c.Assert(err, IsNil)
	c.Assert(ts, Equals, backupts)
}

func (r *testBackup) TestOnBackupRegionErrorResponse(c *C) {
	type Case struct {
		storeID           uint64
		bo                *tikv.Backoffer
		backupTS          uint64
		lockResolver      *txnlock.LockResolver
		resp              *backuppb.BackupResponse
		exceptedBackoffMs int
		exceptedErr       bool
	}
	newBackupRegionErrorResp := func(regionError *errorpb.Error) *backuppb.BackupResponse {
		return &backuppb.BackupResponse{Error: &backuppb.Error{Detail: &backuppb.Error_RegionError{RegionError: regionError}}}
	}

	cases := []Case{
		{storeID: 1, backupTS: 421123291611137, resp: newBackupRegionErrorResp(&errorpb.Error{NotLeader: &errorpb.NotLeader{}}), exceptedBackoffMs: 1000, exceptedErr: false},
		{storeID: 1, backupTS: 421123291611137, resp: newBackupRegionErrorResp(&errorpb.Error{RegionNotFound: &errorpb.RegionNotFound{}}), exceptedBackoffMs: 1000, exceptedErr: false},
		{storeID: 1, backupTS: 421123291611137, resp: newBackupRegionErrorResp(&errorpb.Error{KeyNotInRegion: &errorpb.KeyNotInRegion{}}), exceptedBackoffMs: 0, exceptedErr: true},
		{storeID: 1, backupTS: 421123291611137, resp: newBackupRegionErrorResp(&errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{}}), exceptedBackoffMs: 1000, exceptedErr: false},
		{storeID: 1, backupTS: 421123291611137, resp: newBackupRegionErrorResp(&errorpb.Error{ServerIsBusy: &errorpb.ServerIsBusy{}}), exceptedBackoffMs: 1000, exceptedErr: false},
		{storeID: 1, backupTS: 421123291611137, resp: newBackupRegionErrorResp(&errorpb.Error{StaleCommand: &errorpb.StaleCommand{}}), exceptedBackoffMs: 1000, exceptedErr: false},
		{storeID: 1, backupTS: 421123291611137, resp: newBackupRegionErrorResp(&errorpb.Error{StoreNotMatch: &errorpb.StoreNotMatch{}}), exceptedBackoffMs: 1000, exceptedErr: false},
		{storeID: 1, backupTS: 421123291611137, resp: newBackupRegionErrorResp(&errorpb.Error{RaftEntryTooLarge: &errorpb.RaftEntryTooLarge{}}), exceptedBackoffMs: 0, exceptedErr: true},
		{storeID: 1, backupTS: 421123291611137, resp: newBackupRegionErrorResp(&errorpb.Error{ReadIndexNotReady: &errorpb.ReadIndexNotReady{}}), exceptedBackoffMs: 1000, exceptedErr: false},
		{storeID: 1, backupTS: 421123291611137, resp: newBackupRegionErrorResp(&errorpb.Error{ProposalInMergingMode: &errorpb.ProposalInMergingMode{}}), exceptedBackoffMs: 1000, exceptedErr: false},
	}
	for _, cs := range cases {
		c.Log(cs)
		_, backoffMs, err := backup.OnBackupResponse(cs.storeID, cs.bo, cs.backupTS, cs.lockResolver, cs.resp)
		c.Assert(backoffMs, Equals, cs.exceptedBackoffMs)
		if cs.exceptedErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
		}
	}
}

func (r *testBackup) TestSendCreds(c *C) {
	accessKey := "ab"
	secretAccessKey := "cd"
	backendOpt := storage.BackendOptions{
		S3: storage.S3BackendOptions{
			AccessKey:       accessKey,
			SecretAccessKey: secretAccessKey,
		},
	}
	backend, err := storage.ParseBackend("s3://bucket/prefix/", &backendOpt)
	c.Assert(err, IsNil)
	opts := &storage.ExternalStorageOptions{
		SendCredentials: true,
	}
	_, err = storage.New(r.ctx, backend, opts)
	c.Assert(err, IsNil)
	accessKey = backend.GetS3().AccessKey
	c.Assert(accessKey, Equals, "ab")
	secretAccessKey = backend.GetS3().SecretAccessKey
	c.Assert(secretAccessKey, Equals, "cd")

	backendOpt = storage.BackendOptions{
		S3: storage.S3BackendOptions{
			AccessKey:       accessKey,
			SecretAccessKey: secretAccessKey,
		},
	}
	backend, err = storage.ParseBackend("s3://bucket/prefix/", &backendOpt)
	c.Assert(err, IsNil)
	opts = &storage.ExternalStorageOptions{
		SendCredentials: false,
	}
	_, err = storage.New(r.ctx, backend, opts)
	c.Assert(err, IsNil)
	accessKey = backend.GetS3().AccessKey
	c.Assert(accessKey, Equals, "")
	secretAccessKey = backend.GetS3().SecretAccessKey
	c.Assert(secretAccessKey, Equals, "")
}

func (r *testBackup) TestCheckBackupIsLocked(c *C) {
	ctx := context.Background()

	r.resetStorage(c)
	// check passed with an empty storage
	err := backup.CheckBackupStorageIsLocked(ctx, r.storage)
	c.Assert(err, IsNil)

	// check passed with only a lock file
	err = r.storage.WriteFile(ctx, metautil.LockFile, nil)
	c.Assert(err, IsNil)
	err = backup.CheckBackupStorageIsLocked(ctx, r.storage)
	c.Assert(err, IsNil)

	// check passed with a lock file and other non-sst files.
	err = r.storage.WriteFile(ctx, "1.txt", nil)
	c.Assert(err, IsNil)
	err = backup.CheckBackupStorageIsLocked(ctx, r.storage)
	c.Assert(err, IsNil)

	// check failed
	err = r.storage.WriteFile(ctx, "1.sst", nil)
	c.Assert(err, IsNil)
	err = backup.CheckBackupStorageIsLocked(ctx, r.storage)
	c.Assert(err, ErrorMatches, "backup lock file and sst file exist in(.+)")
}

func (r *testBackup) TestUpdateBRGCSafePoint(c *C) {
	r.SetUpSuite(c)
	duration := time.Duration(1) * time.Minute
	physical, logical, err := r.mockPDClient.GetTS(r.ctx)
	c.Assert(err, IsNil)
	tso := oracle.ComposeTS(physical, logical)
	curTime := oracle.GetTimeFromTS(tso)
	backupAgo := curTime.Add(-duration)
	expectbackupTS := oracle.ComposeTS(oracle.GetPhysical(backupAgo), logical)
	r.backupClient.SetGCTTL(time.Minute)
	c.Assert(r.backupClient.GetCurAPIVersion(), Equals, kvrpcpb.APIVersion_V2)

	backupTs, err := r.backupClient.UpdateBRGCSafePoint(r.ctx, duration)
	c.Assert(err, IsNil)
	c.Assert(backupTs-expectbackupTS, Equals, uint64(1)) // UpdateBRGCSafePoint will get ts again, so there is a gap.

	curSafePoint, err := r.mockPDClient.UpdateServiceGCSafePoint(r.ctx, "test", 100, math.MaxUint64)
	c.Assert(err, IsNil)
	c.Assert(curSafePoint, Equals, backupTs-1)
}
