// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package glue

import (
	"context"

	"github.com/pingcap/tidb/kv"
	pd "github.com/tikv/pd/client"
)

// Glue is an abstraction of TiDB function calls used in BR.
type Glue interface {
	Open(path string, option pd.SecurityOption) (kv.Storage, error)

	// OwnsStorage returns whether the storage returned by Open() is owned
	// If this method returns false, the connection manager will never close the storage.
	OwnsStorage() bool

	StartProgress(ctx context.Context, cmdName string, total int64, redirectLog bool) Progress

	// Record records some information useful for log-less summary.
	Record(name string, value uint64)

	// GetVersion gets BR package version to run backup/restore job
	GetVersion() string
}

// Progress is an interface recording the current execution progress.
type Progress interface {
	// Inc increases the progress. This method must be goroutine-safe, and can
	// be called from any goroutine.
	Inc()
	// Close marks the progress as 100% complete and that Inc() can no longer be
	// called.
	Close()
}
