// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tikv/migration/br/pkg/restore"
)

func TestRestoreConfigAdjust(t *testing.T) {
	cfg := &RestoreConfig{}
	cfg.adjustRestoreConfig()

	require.Equal(t, uint32(defaultRestoreConcurrency), cfg.Config.Concurrency)
	require.Equal(t, defaultSwitchInterval, cfg.Config.SwitchModeInterval)
	require.Equal(t, restore.DefaultMergeRegionKeyCount, cfg.MergeSmallRegionKeyCount)
	require.Equal(t, restore.DefaultMergeRegionSizeBytes, cfg.MergeSmallRegionSizeBytes)
}
