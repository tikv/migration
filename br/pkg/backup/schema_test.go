// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package backup_test

import (
	"context"

	"github.com/golang/protobuf/proto"
	. "github.com/pingcap/check"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/tikv/migration/br/pkg/metautil"
	"github.com/tikv/migration/br/pkg/mock"
	"github.com/tikv/migration/br/pkg/storage"
)

var _ = Suite(&testBackupSchemaSuite{})

type testBackupSchemaSuite struct {
	mock *mock.Cluster
}

func (s *testBackupSchemaSuite) SetUpSuite(c *C) {
	var err error
	s.mock, err = mock.NewCluster()
	c.Assert(err, IsNil)
	c.Assert(s.mock.Start(), IsNil)
}

func (s *testBackupSchemaSuite) TearDownSuite(c *C) {
	s.mock.Stop()
	testleak.AfterTest(c)()
}

func (s *testBackupSchemaSuite) GetRandomStorage(c *C) storage.ExternalStorage {
	base := c.MkDir()
	es, err := storage.NewLocalStorage(base)
	c.Assert(err, IsNil)
	return es
}

func (s *testBackupSchemaSuite) GetSchemasFromMeta(c *C, es storage.ExternalStorage) []*metautil.Table {
	ctx := context.Background()
	metaBytes, err := es.ReadFile(ctx, metautil.MetaFile)
	c.Assert(err, IsNil)
	mockMeta := &backuppb.BackupMeta{}
	err = proto.Unmarshal(metaBytes, mockMeta)
	c.Assert(err, IsNil)
	metaReader := metautil.NewMetaReader(mockMeta,
		es,
		&backuppb.CipherInfo{
			CipherType: encryptionpb.EncryptionMethod_PLAINTEXT,
		},
	)

	output := make(chan *metautil.Table, 4)
	go func() {
		err = metaReader.ReadSchemasFiles(ctx, output)
		c.Assert(err, IsNil)
		close(output)
	}()

	schemas := make([]*metautil.Table, 0, 4)
	for s := range output {
		schemas = append(schemas, s)
	}
	return schemas
}
