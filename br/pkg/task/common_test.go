// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/coreos/go-semver/semver"
	backup "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
	"github.com/tikv/migration/br/pkg/feature"
)

type fakeValue string

func (f fakeValue) String() string {
	return string(f)
}

func (f fakeValue) Set(string) error {
	panic("implement me")
}

func (f fakeValue) Type() string {
	panic("implement me")
}

func TestUrlNoQuery(t *testing.T) {
	flag := &pflag.Flag{
		Name:  flagStorage,
		Value: fakeValue("s3://some/what?secret=a123456789&key=987654321"),
	}
	field := flagToZapField(flag)
	require.Equal(t, flagStorage, field.Key)
	require.Equal(t, "s3://some/what", field.Interface.(fmt.Stringer).String())
}

func TestStripingPDURL(t *testing.T) {
	nor1, err := normalizePDURL("https://pd:5432", true)
	require.NoError(t, err)
	require.Equal(t, "pd:5432", nor1)
	_, err = normalizePDURL("https://pd.pingcap.com", false)
	require.Error(t, err)
	require.Regexp(t, ".*pd url starts with https while TLS disabled.*", err.Error())
	_, err = normalizePDURL("http://127.0.0.1:2379", true)
	require.Error(t, err)
	require.Regexp(t, ".*pd url starts with http while TLS enabled.*", err.Error())
	nor, err := normalizePDURL("http://127.0.0.1", false)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1", nor)
	noChange, err := normalizePDURL("127.0.0.1:2379", false)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1:2379", noChange)
}

func TestCheckCipherKeyMatch(t *testing.T) {
	cases := []struct {
		CipherType encryptionpb.EncryptionMethod
		CipherKey  string
		ok         bool
	}{
		{
			CipherType: encryptionpb.EncryptionMethod_PLAINTEXT,
			ok:         true,
		},
		{
			CipherType: encryptionpb.EncryptionMethod_UNKNOWN,
			ok:         false,
		},
		{
			CipherType: encryptionpb.EncryptionMethod_AES128_CTR,
			CipherKey:  "0123456789abcdef0123456789abcdef",
			ok:         true,
		},
		{
			CipherType: encryptionpb.EncryptionMethod_AES128_CTR,
			CipherKey:  "0123456789abcdef0123456789abcd",
			ok:         false,
		},
		{
			CipherType: encryptionpb.EncryptionMethod_AES192_CTR,
			CipherKey:  "0123456789abcdef0123456789abcdef0123456789abcdef",
			ok:         true,
		},
		{
			CipherType: encryptionpb.EncryptionMethod_AES192_CTR,
			CipherKey:  "0123456789abcdef0123456789abcdef0123456789abcdefff",
			ok:         false,
		},
		{
			CipherType: encryptionpb.EncryptionMethod_AES256_CTR,
			CipherKey:  "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			ok:         true,
		},
		{
			CipherType: encryptionpb.EncryptionMethod_AES256_CTR,
			CipherKey:  "",
			ok:         false,
		},
	}

	for _, c := range cases {
		cipherKey, err := hex.DecodeString(c.CipherKey)
		require.NoError(t, err)
		require.Equal(t, c.ok, checkCipherKeyMatch(&backup.CipherInfo{
			CipherType: c.CipherType,
			CipherKey:  cipherKey,
		}))
	}
}

func TestCheckCipherKey(t *testing.T) {
	cases := []struct {
		cipherKey string
		keyFile   string
		ok        bool
	}{
		{
			cipherKey: "0123456789abcdef0123456789abcdef",
			keyFile:   "",
			ok:        true,
		},
		{
			cipherKey: "0123456789abcdef0123456789abcdef",
			keyFile:   "/tmp/abc",
			ok:        false,
		},
		{
			cipherKey: "",
			keyFile:   "/tmp/abc",
			ok:        true,
		},
		{
			cipherKey: "",
			keyFile:   "",
			ok:        false,
		},
	}

	for _, c := range cases {
		err := checkCipherKey(c.cipherKey, c.keyFile)
		if c.ok {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
		}
	}
}

func TestCheckBackupAPIVersion(t *testing.T) {
	featureGate := feature.NewFeatureGate(semver.New("6.1.0"))
	require.Equal(t, CheckBackupAPIVersion(featureGate, kvrpcpb.APIVersion_V1, kvrpcpb.APIVersion_V1), true)
	require.Equal(t, CheckBackupAPIVersion(featureGate, kvrpcpb.APIVersion_V1TTL, kvrpcpb.APIVersion_V1TTL), true)
	require.Equal(t, CheckBackupAPIVersion(featureGate, kvrpcpb.APIVersion_V2, kvrpcpb.APIVersion_V2), true)
	require.Equal(t, CheckBackupAPIVersion(featureGate, kvrpcpb.APIVersion_V1, kvrpcpb.APIVersion_V2), true)
	require.Equal(t, CheckBackupAPIVersion(featureGate, kvrpcpb.APIVersion_V1TTL, kvrpcpb.APIVersion_V2), true)
	require.Equal(t, CheckBackupAPIVersion(featureGate, kvrpcpb.APIVersion_V1, kvrpcpb.APIVersion_V1TTL), false)
	require.Equal(t, CheckBackupAPIVersion(featureGate, kvrpcpb.APIVersion_V1TTL, kvrpcpb.APIVersion_V1), false)
	require.Equal(t, CheckBackupAPIVersion(featureGate, kvrpcpb.APIVersion_V2, kvrpcpb.APIVersion_V1), false)
	require.Equal(t, CheckBackupAPIVersion(featureGate, kvrpcpb.APIVersion_V2, kvrpcpb.APIVersion_V1TTL), false)
}
