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
	"context"
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type mockKvCLient struct {
	tikvpb.TikvClient
}

func getStubChecksum() Checksum {
	return Checksum{
		Crc64Xor:   1234,
		TotalKvs:   10,
		TotalBytes: 100,
	}
}

func (m *mockKvCLient) RawChecksum(ctx context.Context, in *kvrpcpb.RawChecksumRequest, opts ...grpc.CallOption) (*kvrpcpb.RawChecksumResponse, error) {
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
	mockClient := &mockKvCLient{}
	files := []*backuppb.File{&backuppb.File{
		Crc64Xor:   stubChecksum.Crc64Xor,
		TotalKvs:   stubChecksum.TotalKvs,
		TotalBytes: stubChecksum.TotalBytes,
	}}
	err := NewChecksumExecutor("", "", files, mockClient, 1, StorageChecksumCommand).Execute(ctx)
	require.Equal(t, err, nil)
}
