package feature

import "github.com/coreos/go-semver/semver"

type Feature int

const (
	APIVersionConversion Feature = iota
	Checksum
	BackupTs
)

var (
	minAPIVersionConversionVersion = semver.New("6.1.0")
	minChecksumVersion             = semver.New("6.1.0")
	minBackupTsVersion             = semver.New("6.1.0")
)

type FeatureGate struct {
	features  map[Feature]*semver.Version
	pdVersion *semver.Version
}

func NewFeatureGate(pdVersion *semver.Version) *FeatureGate {
	featureGate := new(FeatureGate)
	featureGate.features = make(map[Feature]*semver.Version)
	featureGate.features[APIVersionConversion] = minAPIVersionConversionVersion
	featureGate.features[Checksum] = minChecksumVersion
	featureGate.features[BackupTs] = minBackupTsVersion
	featureGate.pdVersion = pdVersion
	return featureGate
}

func (f *FeatureGate) IsEnabled(feature Feature) bool {
	return f.pdVersion.Compare(*f.features[feature]) >= 0
}
