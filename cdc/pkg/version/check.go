// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package version

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	cerror "github.com/tikv/migration/cdc/pkg/errors"
	"github.com/tikv/migration/cdc/pkg/httputil"
	"github.com/tikv/migration/cdc/pkg/security"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

var (
	// minPDVersion is the version of the minimal compatible PD.
	minPDVersion *semver.Version = semver.New("6.2.0-alpah")
	// maxPDVersion is the version of the maximum compatible PD.
	// Compatible versions are in [minPDVersion, maxPDVersion)
	// 9999.0.0 disables the check effectively in the master branch.
	maxPDVersion *semver.Version = semver.New("9999.0.0")

	// MinTiKVVersion is the version of the minimal compatible TiKV.
	MinTiKVVersion *semver.Version = semver.New("6.2.0-alpah")
	// maxTiKVVersion is the version of the maximum compatible TiKV.
	// Compatible versions are in [MinTiKVVersion, maxTiKVVersion)
	// 9999.0.0 disables the check effectively in the master branch.
	maxTiKVVersion *semver.Version = semver.New("9999.0.0")

	// minTiKVCDCVersion is the version of the minimal compatible TiKVCDC.
	minTiKVCDCVersion *semver.Version = semver.New("1.0.0")
	// Compatible versions are in [MinTiKVCDCVersion, MaxTiKVCDCVersion)
	// 9999.0.0 disables the check effectively in the master branch.
	maxTiKVCDCVersion *semver.Version = semver.New("9999.0.0")

	// CaptureInfo.Version is added since v4.0.11,
	// we use the minimal release version as default.
	defaultTiKVCDCVersion *semver.Version = semver.New("1.0.0")
)

var versionHash = regexp.MustCompile("-[0-9]+-g[0-9a-f]{7,}(-dev)?")

func removeVAndHash(v string) string {
	if v == "" {
		return v
	}
	v = versionHash.ReplaceAllLiteralString(v, "")
	v = strings.TrimSuffix(v, "-dirty")
	return strings.TrimPrefix(v, "v")
}

// CheckClusterVersion check TiKV and PD version.
// need only one PD alive and match the cdc version.
func CheckClusterVersion(
	ctx context.Context, client pd.Client, pdAddrs []string, credential *security.Credential, errorTiKVIncompat bool,
) error {
	err := CheckStoreVersion(ctx, client, 0 /* check all TiKV */)
	if err != nil {
		if errorTiKVIncompat {
			return err
		}
		log.Warn("check TiKV version failed", zap.Error(err))
	}

	for _, pdAddr := range pdAddrs {
		err = CheckPDVersion(ctx, pdAddr, credential)
		if err == nil {
			break
		}
	}

	return err
}

// CheckPDVersion check PD version.
func CheckPDVersion(ctx context.Context, pdAddr string, credential *security.Credential) error {
	// See more: https://github.com/pingcap/pd/blob/v4.0.0-rc.1/server/api/version.go
	pdVer := struct {
		Version string `json:"version"`
	}{}

	httpClient, err := httputil.NewClient(credential)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(
		ctx, http.MethodGet, fmt.Sprintf("%s/pd/api/v1/version", pdAddr), nil)
	if err != nil {
		return cerror.WrapError(cerror.ErrCheckClusterVersionFromPD, err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return cerror.WrapError(cerror.ErrCheckClusterVersionFromPD, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		arg := fmt.Sprintf("response status: %s", resp.Status)
		return cerror.ErrCheckClusterVersionFromPD.GenWithStackByArgs(arg)
	}

	content, err := io.ReadAll(resp.Body)
	if err != nil {
		return cerror.WrapError(cerror.ErrCheckClusterVersionFromPD, err)
	}

	err = json.Unmarshal(content, &pdVer)
	if err != nil {
		return cerror.WrapError(cerror.ErrCheckClusterVersionFromPD, err)
	}

	ver, err := semver.NewVersion(removeVAndHash(pdVer.Version))
	if err != nil {
		return cerror.WrapError(cerror.ErrNewSemVersion, err)
	}

	minOrd := ver.Compare(*minPDVersion)
	if minOrd < 0 {
		arg := fmt.Sprintf("PD %s is not supported, the minimal compatible version is %s",
			removeVAndHash(pdVer.Version), minPDVersion)
		return cerror.ErrVersionIncompatible.GenWithStackByArgs(arg)
	}
	maxOrd := ver.Compare(*maxPDVersion)
	if maxOrd >= 0 {
		arg := fmt.Sprintf("PD %s is not supported, the maximum compatible version is %s",
			removeVAndHash(pdVer.Version), maxPDVersion)
		return cerror.ErrVersionIncompatible.GenWithStackByArgs(arg)
	}
	return nil
}

// CheckStoreVersion checks whether the given TiKV is compatible with this CDC.
// If storeID is 0, it checks all TiKV.
func CheckStoreVersion(ctx context.Context, client pd.Client, storeID uint64) error {
	var stores []*metapb.Store
	var err error
	if storeID == 0 {
		stores, err = client.GetAllStores(ctx, pd.WithExcludeTombstone())
	} else {
		stores = make([]*metapb.Store, 1)
		stores[0], err = client.GetStore(ctx, storeID)
	}
	if err != nil {
		return cerror.WrapError(cerror.ErrGetAllStoresFailed, err)
	}

	for _, s := range stores {
		ver, err := semver.NewVersion(removeVAndHash(s.Version))
		if err != nil {
			return cerror.WrapError(cerror.ErrNewSemVersion, err)
		}
		minOrd := ver.Compare(*MinTiKVVersion)
		if minOrd < 0 {
			arg := fmt.Sprintf("TiKV %s is not supported, the minimal compatible version is %s",
				removeVAndHash(s.Version), MinTiKVVersion)
			return cerror.ErrVersionIncompatible.GenWithStackByArgs(arg)
		}
		maxOrd := ver.Compare(*maxTiKVVersion)
		if maxOrd >= 0 {
			arg := fmt.Sprintf("TiKV %s is not supported, the maximum compatible version is %s",
				removeVAndHash(s.Version), maxTiKVVersion)
			return cerror.ErrVersionIncompatible.GenWithStackByArgs(arg)
		}
	}
	return nil
}

// TiKVCDCClusterVersion is the version of TiKVCDC cluster
type TiKVCDCClusterVersion struct {
	*semver.Version
}

// IsUnknown returns whether this is an unknown version
func (v *TiKVCDCClusterVersion) IsUnknown() bool {
	return v.Version == nil
}

// TiKVCDCClusterVersionUnknown is a read-only variable to represent the unknown cluster version
var TiKVCDCClusterVersionUnknown = TiKVCDCClusterVersion{}

// GetTiKVCDCClusterVersion returns the version of ticdc cluster
func GetTiKVCDCClusterVersion(captureVersion []string) (TiKVCDCClusterVersion, error) {
	if len(captureVersion) == 0 {
		return TiKVCDCClusterVersionUnknown, nil
	}
	var minVer *semver.Version
	for _, versionStr := range captureVersion {
		var ver *semver.Version
		var err error
		if versionStr != "" {
			ver, err = semver.NewVersion(removeVAndHash(versionStr))
		} else {
			ver = defaultTiKVCDCVersion
		}
		if err != nil {
			return TiKVCDCClusterVersionUnknown, cerror.WrapError(cerror.ErrNewSemVersion, err)
		}
		if minVer == nil || ver.Compare(*minVer) < 0 {
			minVer = ver
		}
	}
	return TiKVCDCClusterVersion{minVer}, nil
}

// CheckTiKVCDCClusterVersion returns the version of ticdc cluster
func CheckTiKVCDCClusterVersion(cdcClusterVer TiKVCDCClusterVersion) (unknown bool, err error) {
	if cdcClusterVer.Version == nil {
		return true, nil
	}
	ver := cdcClusterVer.Version
	minOrd := ver.Compare(*minTiKVCDCVersion)
	if minOrd < 0 {
		arg := fmt.Sprintf("TiKVCDC %s is not supported, the minimal compatible version is %s"+
			"try tiup ctl:%s tikv-cdc [COMMAND]",
			ver, minTiKVCDCVersion, ver)
		return false, cerror.ErrVersionIncompatible.GenWithStackByArgs(arg)
	}
	maxOrd := ver.Compare(*maxTiKVCDCVersion)
	if maxOrd >= 0 {
		arg := fmt.Sprintf("TiKVCDC %s is not supported, the maximum compatible version is %s"+
			"try tiup ctl:%s tikv-cdc [COMMAND]",
			ver, maxTiKVCDCVersion, ver)
		return false, cerror.ErrVersionIncompatible.GenWithStackByArgs(arg)
	}
	return false, nil
}
