package feature

import "github.com/coreos/go-semver/semver"

type Feature int

const (
	APIVersionConversion Feature = iota
	Checksum
	BackupTs
	SplitRegion
)

var (
	minAPIVersionConversionVersion = semver.New("6.1.0")
	minChecksumVersion             = semver.New("6.1.1")
	minBackupTsVersion             = semver.New("6.2.0")
	minSplitRegionVersion          = semver.New("5.2.0")
)

type Gate struct {
	features  map[Feature]*semver.Version
	pdVersion *semver.Version
}

func NewFeatureGate(pdVersion *semver.Version) *Gate {
	featureGate := new(Gate)
	featureGate.features = make(map[Feature]*semver.Version)
	featureGate.features[APIVersionConversion] = minAPIVersionConversionVersion
	featureGate.features[Checksum] = minChecksumVersion
	featureGate.features[BackupTs] = minBackupTsVersion
	featureGate.features[SplitRegion] = minSplitRegionVersion
	featureGate.pdVersion = pdVersion
	return featureGate
}

func (f *Gate) IsEnabled(feature Feature) bool {
	return f.pdVersion.Compare(*f.features[feature]) >= 0
}
