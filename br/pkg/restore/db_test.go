// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore_test

import (
	"context"
	"math"
	"strconv"
	"testing"

	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/migration/br/pkg/gluetidb"
	"github.com/tikv/migration/br/pkg/metautil"
	"github.com/tikv/migration/br/pkg/mock"
	"github.com/tikv/migration/br/pkg/restore"
	"github.com/tikv/migration/br/pkg/storage"
)

type testRestoreSchemaSuite struct {
	mock    *mock.Cluster
	storage storage.ExternalStorage
}

func createRestoreSchemaSuite(t *testing.T) (s *testRestoreSchemaSuite, clean func()) {
	var err error
	s = new(testRestoreSchemaSuite)
	s.mock, err = mock.NewCluster()
	require.NoError(t, err)
	base := t.TempDir()
	s.storage, err = storage.NewLocalStorage(base)
	require.NoError(t, err)
	require.NoError(t, s.mock.Start())
	clean = func() {
		s.mock.Stop()
	}
	return
}

func TestRestoreAutoIncID(t *testing.T) {
	s, clean := createRestoreSchemaSuite(t)
	defer clean()
	tk := testkit.NewTestKit(t, s.mock.Storage)
	tk.MustExec("use test")
	tk.MustExec("set @@sql_mode=''")
	tk.MustExec("drop table if exists `\"t\"`;")
	// Test SQL Mode
	tk.MustExec("create table `\"t\"` (" +
		"a int not null," +
		"time timestamp not null default '0000-00-00 00:00:00');",
	)
	tk.MustExec("insert into `\"t\"` values (10, '0000-00-00 00:00:00');")
	// Query the current AutoIncID
	autoIncID, err := strconv.ParseUint(tk.MustQuery("admin show `\"t\"` next_row_id").Rows()[0][3].(string), 10, 64)
	require.NoErrorf(t, err, "Error query auto inc id: %s", err)
	// Get schemas of db and table
	info, err := s.mock.Domain.GetSnapshotInfoSchema(math.MaxUint64)
	require.NoErrorf(t, err, "Error get snapshot info schema: %s", err)
	dbInfo, exists := info.SchemaByName(model.NewCIStr("test"))
	require.Truef(t, exists, "Error get db info")
	tableInfo, err := info.TableByName(model.NewCIStr("test"), model.NewCIStr("\"t\""))
	require.NoErrorf(t, err, "Error get table info: %s", err)
	table := metautil.Table{
		Info: tableInfo.Meta(),
		DB:   dbInfo,
	}
	// Get the next AutoIncID
	idAlloc := autoid.NewAllocator(s.mock.Storage, dbInfo.ID, table.Info.ID, false, autoid.RowIDAllocType)
	globalAutoID, err := idAlloc.NextGlobalAutoID()
	require.NoErrorf(t, err, "Error allocate next auto id")
	require.Equal(t, uint64(globalAutoID), autoIncID)
	// Alter AutoIncID to the next AutoIncID + 100
	table.Info.AutoIncID = globalAutoID + 100
	db, err := restore.NewDB(gluetidb.New(), s.mock.Storage)
	require.NoErrorf(t, err, "Error create DB")
	tk.MustExec("drop database if exists test;")
	// Test empty collate value
	table.DB.Charset = "utf8mb4"
	table.DB.Collate = ""
	err = db.CreateDatabase(context.Background(), table.DB)
	require.NoErrorf(t, err, "Error create empty collate db: %s %s", err, s.mock.DSN)
	tk.MustExec("drop database if exists test;")
	// Test empty charset value
	table.DB.Charset = ""
	table.DB.Collate = "utf8mb4_bin"
	err = db.CreateDatabase(context.Background(), table.DB)
	require.NoErrorf(t, err, "Error create empty charset db: %s %s", err, s.mock.DSN)
	uniqueMap := make(map[restore.UniqueTableName]bool)
	err = db.CreateTable(context.Background(), &table, uniqueMap)
	require.NoErrorf(t, err, "Error create table: %s %s", err, s.mock.DSN)

	tk.MustExec("use test")
	autoIncID, err = strconv.ParseUint(tk.MustQuery("admin show `\"t\"` next_row_id").Rows()[0][3].(string), 10, 64)
	require.NoErrorf(t, err, "Error query auto inc id: %s", err)
	// Check if AutoIncID is altered successfully.
	require.Equal(t, uint64(globalAutoID+100), autoIncID)

	// try again, failed due to table exists.
	table.Info.AutoIncID = globalAutoID + 200
	err = db.CreateTable(context.Background(), &table, uniqueMap)
	// Check if AutoIncID is not altered.
	autoIncID, err = strconv.ParseUint(tk.MustQuery("admin show `\"t\"` next_row_id").Rows()[0][3].(string), 10, 64)
	require.NoErrorf(t, err, "Error query auto inc id: %s", err)
	require.Equal(t, uint64(globalAutoID+100), autoIncID)

	// try again, success because we use alter sql in unique map.
	table.Info.AutoIncID = globalAutoID + 300
	uniqueMap[restore.UniqueTableName{"test", "\"t\""}] = true
	err = db.CreateTable(context.Background(), &table, uniqueMap)
	// Check if AutoIncID is altered to globalAutoID + 300.
	autoIncID, err = strconv.ParseUint(tk.MustQuery("admin show `\"t\"` next_row_id").Rows()[0][3].(string), 10, 64)
	require.NoErrorf(t, err, "Error query auto inc id: %s", err)
	require.Equal(t, uint64(globalAutoID+300), autoIncID)

}
