// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package version

import (
	"context"
	"testing"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/migration/br/pkg/version/build"
	pd "github.com/tikv/pd/client"
)

type mockPDClient struct {
	pd.Client
	getAllStores func() []*metapb.Store
}

func (m *mockPDClient) GetAllStores(ctx context.Context, opts ...pd.GetStoreOption) ([]*metapb.Store, error) {
	if m.getAllStores != nil {
		return m.getAllStores(), nil
	}
	return []*metapb.Store{}, nil
}

func tiflash(version string) []*metapb.Store {
	return []*metapb.Store{
		{Version: version, Labels: []*metapb.StoreLabel{{Key: "engine", Value: "tiflash"}}},
	}
}

func TestCheckClusterVersion(t *testing.T) {
	oldReleaseVersion := build.ReleaseVersion
	defer func() {
		build.ReleaseVersion = oldReleaseVersion
	}()

	mock := mockPDClient{
		Client: nil,
	}

	{
		build.ReleaseVersion = "br-v0.1"
		mock.getAllStores = func() []*metapb.Store {
			return tiflash("v4.0.0-rc.1")
		}
		err := CheckClusterVersion(context.Background(), &mock, CheckVersionForBR)
		require.Error(t, err)
		require.Regexp(t, `TiFlash.* does not support BR`, err.Error())
	}

	{
		build.ReleaseVersion = "br-v3.1.0-beta.2"
		mock.getAllStores = func() []*metapb.Store {
			return []*metapb.Store{{Version: minTiKVVersion.String()}}
		}
		err := CheckClusterVersion(context.Background(), &mock, CheckVersionForBR)
		require.NoError(t, err)
	}

	{
		build.ReleaseVersion = "rb-v3.1.0-beta.2"
		mock.getAllStores = func() []*metapb.Store {
			return []*metapb.Store{{Version: minTiKVVersion.String()}}
		}
		err := CheckClusterVersion(context.Background(), &mock, CheckVersionForBR)
		require.Regexp(t, `rb is not in dotted-tri format`, err.Error())
	}

	{
		build.ReleaseVersion = "br-v0.1.0"
		mock.getAllStores = func() []*metapb.Store {
			return []*metapb.Store{{Version: "v6.1.0"}}
		}
		err := CheckClusterVersion(context.Background(), &mock, CheckVersionForBR)
		require.NoError(t, err)
	}

	{
		build.ReleaseVersion = "v0.1.0"
		mock.getAllStores = func() []*metapb.Store {
			return []*metapb.Store{{Version: "v6.2.0"}}
		}
		err := CheckClusterVersion(context.Background(), &mock, CheckVersionForBR)
		require.NoError(t, err)
	}
}

func TestCompareVersion(t *testing.T) {
	require.Equal(t, -1, semver.New("4.0.0-rc").Compare(*semver.New("4.0.0-rc.2")))
	require.Equal(t, -1, semver.New("4.0.0-beta.3").Compare(*semver.New("4.0.0-rc.2")))
	require.Equal(t, -1, semver.New("4.0.0-rc.1").Compare(*semver.New("4.0.0")))
	require.Equal(t, -1, semver.New("4.0.0-beta.1").Compare(*semver.New("4.0.0")))
	require.Equal(t, -1, semver.New(removeVAndHash("4.0.0-rc-35-g31dae220")).Compare(*semver.New("4.0.0-rc.2")))
	require.Equal(t, 1, semver.New(removeVAndHash("4.0.0-9-g30f0b014")).Compare(*semver.New("4.0.0-rc.1")))
	require.Equal(t, 0, semver.New(removeVAndHash("v3.0.0-beta-211-g09beefbe0-dirty")).
		Compare(*semver.New("3.0.0-beta")))
	require.Equal(t, 0, semver.New(removeVAndHash("v3.0.5-dirty")).
		Compare(*semver.New("3.0.5")))
	require.Equal(t, 0, semver.New(removeVAndHash("v3.0.5-beta.12-dirty")).
		Compare(*semver.New("3.0.5-beta.12")))
	require.Equal(t, 0, semver.New(removeVAndHash("v2.1.0-rc.1-7-g38c939f-dirty")).
		Compare(*semver.New("2.1.0-rc.1")))
}
