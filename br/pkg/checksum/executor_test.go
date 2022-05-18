// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package checksum

import (
	"testing"

	"github.com/stretchr/testify/require"
)

/*
type mockPdCLient struct {
	pd.Client
}

func getStubChecksum() Checksum {
	return Checksum{
		Crc64Xor:   1234,
		TotalKvs:   10,
		TotalBytes: 100,
	}
}

func (m *mockPdCLient) RawChecksum(ctx context.Context, in *kvrpcpb.RawChecksumRequest, opts ...grpc.CallOption) (*kvrpcpb.RawChecksumResponse, error) {
	stubChecksum := getStubChecksum()
	response := kvrpcpb.RawChecksumResponse{}
	response.Checksum = stubChecksum.Crc64Xor
	response.TotalKvs = stubChecksum.TotalKvs
	response.TotalBytes = stubChecksum.TotalBytes
	return &response, nil
}

func TestChecksumExecutor(t *testing.T) {
	stubChecksum := getStubChecksum()
	ctx := context.Background()
	mockClient := &mockPdCLient{}
	err := NewExecutor([]byte(""), []byte(""), kvrpcpb.APIVersion_V2, mockClient, 1).
		Execute(ctx, stubChecksum, StorageChecksumCommand, nil)
	require.Equal(t, err, nil)
}*/

func TestAdjustRegionRange(t *testing.T) {
	start, end := adjustRegionRange([]byte(""), []byte(""), []byte(""), []byte(""))
	require.Equal(t, string(start), "")
	require.Equal(t, string(end), "")
	start, end = adjustRegionRange([]byte(""), []byte(""), []byte("a"), []byte("z"))
	require.Equal(t, string(start), "a")
	require.Equal(t, string(end), "z")
	start, end = adjustRegionRange([]byte("a"), []byte("z"), []byte(""), []byte(""))
	require.Equal(t, string(start), "a")
	require.Equal(t, string(end), "z")
	start, end = adjustRegionRange([]byte("a"), []byte(""), []byte(""), []byte("z"))
	require.Equal(t, string(start), "a")
	require.Equal(t, string(end), "z")
	start, end = adjustRegionRange([]byte(""), []byte("z"), []byte("a"), []byte(""))
	require.Equal(t, string(start), "a")
	require.Equal(t, string(end), "z")
}
