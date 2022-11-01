package feature

import (
	"testing"

	"github.com/coreos/go-semver/semver"
	"github.com/stretchr/testify/require"
)

func TestFeatureGate(t *testing.T) {
	gate := NewFeatureGate(semver.New("6.0.0"))

	enabled := gate.IsEnabled(APIVersionConversion)
	require.False(t, enabled)
	enabled = gate.IsEnabled(Checksum)
	require.False(t, enabled)
	enabled = gate.IsEnabled(BackupTs)
	require.False(t, enabled)

	gate = NewFeatureGate(semver.New("6.1.0"))
	enabled = gate.IsEnabled(APIVersionConversion)
	require.True(t, enabled)
	enabled = gate.IsEnabled(Checksum)
	require.True(t, enabled)
	enabled = gate.IsEnabled(BackupTs)
	require.True(t, enabled)
}
