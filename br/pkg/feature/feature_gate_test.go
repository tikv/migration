package feature

import (
	"testing"

	"github.com/coreos/go-semver/semver"
	"github.com/stretchr/testify/require"
)

func TestFeatureGate(t *testing.T) {
	gate := NewFeatureGate(semver.New("6.0.0"))

	require.False(t, gate.IsEnabled(APIVersionConversion))
	require.False(t, gate.IsEnabled(Checksum))
	require.False(t, gate.IsEnabled(BackupTs))
	require.True(t, gate.IsEnabled(SplitRegion))

	gate = NewFeatureGate(semver.New("6.1.0"))
	require.True(t, gate.IsEnabled(APIVersionConversion))
	require.True(t, gate.IsEnabled(Checksum))
	require.True(t, gate.IsEnabled(BackupTs))

	gate = NewFeatureGate(semver.New("5.1.0"))
	require.False(t, gate.IsEnabled(SplitRegion))

	gate = NewFeatureGate(semver.New("5.2.0"))
	require.True(t, gate.IsEnabled(SplitRegion))
}
