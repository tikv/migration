// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package gluetikv

import (
	"context"

	"github.com/tikv/migration/br/pkg/glue"
	"github.com/tikv/migration/br/pkg/summary"
	"github.com/tikv/migration/br/pkg/utils"
	"github.com/tikv/migration/br/pkg/version/build"
)

// Glue is an implementation of glue.Glue that accesses only TiKV without TiDB.
type Glue struct{}

// OwnsStorage implements glue.Glue.
func (Glue) OwnsStorage() bool {
	return true
}

// StartProgress implements glue.Glue.
func (Glue) StartProgress(ctx context.Context, cmdName string, total int64, redirectLog bool) glue.Progress {
	return utils.StartProgress(ctx, cmdName, total, redirectLog, nil)
}

// Record implements glue.Glue.
func (Glue) Record(name string, val uint64) {
	summary.CollectSuccessUnit(name, 1, val)
}

// GetVersion implements glue.Glue.
func (Glue) GetVersion() string {
	return "BR\n" + build.Info()
}
