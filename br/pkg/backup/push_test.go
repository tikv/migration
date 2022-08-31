package backup

import (
	"context"
	"io"
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	pd "github.com/tikv/pd/client"
	"google.golang.org/grpc"
)

var (
	testBackupStart []byte = []byte("ra")
	testBackupEnd   []byte = []byte("rz")
)

type mockBackupBackupClient struct {
	grpc.ClientStream
	req   *backuppb.BackupRequest
	first bool
}

func (x *mockBackupBackupClient) Recv() (*backuppb.BackupResponse, error) {
	if !x.first {
		return nil, io.EOF
	}
	m := new(backuppb.BackupResponse)
	m.StartKey = x.req.StartKey
	m.EndKey = x.req.EndKey
	m.Files = []*backuppb.File{
		{
			Name: string(x.req.StartKey) + ".sst",
		},
	}
	x.first = false
	return m, nil
}

func (x *mockBackupBackupClient) CloseSend() error {
	return nil
}

type mockBackupClient struct {
	client *mockBackupBackupClient
}

func (c *mockBackupClient) Backup(ctx context.Context, in *backuppb.BackupRequest, opts ...grpc.CallOption) (backuppb.Backup_BackupClient, error) {
	c.client.req = in
	c.client.first = true
	return c.client, nil
}

func NewMockBackupClient() (*mockBackupClient, error) {
	return &mockBackupClient{
		client: new(mockBackupBackupClient),
	}, nil
}

type mockBackupMgr struct {
	backupClient *mockBackupClient
	pdClient     pd.Client
}

func (mgr *mockBackupMgr) GetBackupClient(ctx context.Context, storeID uint64) (backuppb.BackupClient, error) {
	return mgr.backupClient, nil
}

func (mgr *mockBackupMgr) ResetBackupClient(ctx context.Context, storeID uint64) (backuppb.BackupClient, error) {
	return mgr.backupClient, nil
}

func (mgr *mockBackupMgr) GetPDClient() pd.Client {
	return mgr.pdClient
}
func (mgr *mockBackupMgr) GetLockResolver() *txnlock.LockResolver {
	return nil
}

func (mgr *mockBackupMgr) Close() {
}

func newMockBackupMgr() (*mockBackupMgr, error) {
	client, err := NewMockBackupClient()
	if err != nil {
		return nil, err
	}
	return &mockBackupMgr{
		backupClient: client,
	}, nil
}

func TestBanckupRaw(t *testing.T) {
	ctx := context.Background()
	mgr, err := newMockBackupMgr()
	require.Nil(t, err)
	pushDown := newPushDown(mgr, 1)

	progressCallback := func(unit ProgressUnit) {}

	rgTree, err := pushDown.pushBackup(ctx, backuppb.BackupRequest{
		StartKey:      testBackupStart,
		EndKey:        []byte("rc"),
		DstApiVersion: kvrpcpb.APIVersion_V1,
	},
		[]*metapb.Store{
			{
				Id:    100,
				State: metapb.StoreState_Up,
			},
		},
		progressCallback,
	)
	require.Nil(t, err)
	require.Equal(t, len(rgTree.GetIncompleteRange(testBackupStart, testBackupEnd)), 1)
}
