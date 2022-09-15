// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package version

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	berrors "github.com/tikv/migration/br/pkg/errors"
	"github.com/tikv/migration/br/pkg/version/build"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

var (
	minTiKVVersion = semver.New("6.1.0-alpha")
	versionHash    = regexp.MustCompile("-[0-9]+-g[0-9a-f]{7,}")
)

// removeVAndHash sanitizes a version string.
func removeVAndHash(v string) string {
	v = versionHash.ReplaceAllLiteralString(v, "")
	v = strings.TrimSuffix(v, "-dirty")
	v = strings.TrimPrefix(v, "br-") // tag is named with br-vx.x.x
	return strings.TrimPrefix(v, "v")
}

// IsTiFlash tests whether the store is based on tiflash engine.
func IsTiFlash(store *metapb.Store) bool {
	for _, label := range store.Labels {
		if label.Key == "engine" && label.Value == "tiflash" {
			return true
		}
	}
	return false
}

// VerChecker is a callback for the CheckClusterVersion, decides whether the cluster is suitable to execute restore.
// See also: CheckVersionForBackup and CheckVersionForBR.
type VerChecker func(store *metapb.Store, ver *semver.Version) error

// CheckClusterVersion check TiKV version.
func CheckClusterVersion(ctx context.Context, client pd.Client, checker VerChecker) error {
	stores, err := client.GetAllStores(ctx, pd.WithExcludeTombstone())
	if err != nil {
		return errors.Trace(err)
	}
	for _, s := range stores {
		isTiFlash := IsTiFlash(s)
		log.Debug("checking compatibility of store in cluster",
			zap.Uint64("ID", s.GetId()),
			zap.Bool("TiFlash?", isTiFlash),
			zap.String("address", s.GetAddress()),
			zap.String("version", s.GetVersion()),
		)
		if isTiFlash {
			return errors.Annotatef(berrors.ErrVersionMismatch, "TiFlash node %s does not support BR", s.GetAddress())
		}

		tikvVersionString := removeVAndHash(s.Version)
		tikvVersion, getVersionErr := semver.NewVersion(tikvVersionString)
		if getVersionErr != nil {
			return errors.Annotatef(berrors.ErrVersionMismatch, "%s: TiKV node %s version %s is invalid", getVersionErr, s.Address, tikvVersionString)
		}
		if checkerErr := checker(s, tikvVersion); checkerErr != nil {
			return checkerErr
		}
	}
	return nil
}

// CheckVersionForBR checks whether version of the cluster and BR itself is compatible.
func CheckVersionForBR(s *metapb.Store, tikvVersion *semver.Version) error {
	BRVersion, err := semver.NewVersion(removeVAndHash(build.ReleaseVersion))
	if err != nil {
		return errors.Annotatef(berrors.ErrVersionMismatch, "%s: invalid version, please recompile using `git fetch origin --tags && make build`", err)
	}

	if tikvVersion.Compare(*minTiKVVersion) < 0 {
		return errors.Annotatef(berrors.ErrVersionMismatch, "TiKV node %s version %s don't support BR, please upgrade cluster to %v",
			s.Address, tikvVersion, *minTiKVVersion)
	}

	// don't warn if we are the master build, which always have the version v4.0.0-beta.2-*
	if build.GitBranch != "master" && tikvVersion.Compare(*BRVersion) > 0 {
		log.Warn(fmt.Sprintf("BR version is outdated, please consider use version %s of BR", tikvVersion))
	}
	return nil
}
