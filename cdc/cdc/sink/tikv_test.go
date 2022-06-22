// Copyright 2022 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sink

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"testing"

	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	tikvconfig "github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/rawkv"
	"github.com/tikv/migration/cdc/cdc/model"
	"github.com/tikv/migration/cdc/pkg/util/testleak"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

type mockRawKVClient struct {
	b *bytes.Buffer
}

func (c *mockRawKVClient) String() string {
	return c.b.String()
}

func (c *mockRawKVClient) Reset() {
	c.b.Reset()
}

func (c *mockRawKVClient) BatchPutWithTTL(ctx context.Context, keys, values [][]byte, ttls []uint64, options ...rawkv.RawOption) error {
	for i, k := range keys {
		fmt.Fprintf(c.b, "1,%s,%s|", string(k), string(values[i]))
	}
	fmt.Fprintf(c.b, "\n")
	return nil
}

func (c *mockRawKVClient) BatchDelete(ctx context.Context, keys [][]byte, options ...rawkv.RawOption) error {
	for _, k := range keys {
		fmt.Fprintf(c.b, "2,%s,|", string(k))
	}
	fmt.Fprintf(c.b, "\n")
	return nil
}

func (c *mockRawKVClient) Close() error {
	return nil
}

var _ rawkvClient = &mockRawKVClient{}

func TestTiKVSinkConfig(t *testing.T) {
	defer testleak.AfterTestT(t)()

	require := require.New(t)

	uri := "tikv://127.0.0.1:1001,127.0.0.2:1002/?concurrency=10"
	sinkURI, err := url.Parse(uri)
	require.NoError(err)

	opts := make(map[string]string)
	_, pdAddr, err := parseTiKVUri(sinkURI, opts)
	require.NoError(err)
	require.Len(pdAddr, 2)
	require.Equal([]string{"http://127.0.0.1:1001", "http://127.0.0.2:1002"}, pdAddr)
	require.Equal("10", opts["concurrency"])
}

func TestTiKVSinkBatcher(t *testing.T) {
	defer testleak.AfterTestT(t)()
	require := require.New(t)

	batcher := tikvBatcher{}
	keys := []string{
		"a", "b", "c", "a",
	}
	values := []string{
		"1", "2", "3", "",
	}
	opTypes := []model.OpType{
		model.OpTypePut, model.OpTypePut, model.OpTypePut, model.OpTypeDelete,
	}
	for i := range keys {
		entry := &model.RawKVEntry{
			OpType: opTypes[i],
			Key:    []byte(keys[i]),
			Value:  []byte(values[i]),
			CRTs:   uint64(i),
		}
		batcher.Append(entry)
	}
	require.Len(batcher.Batches, 2)
	require.Equal(4, batcher.Count())
	require.Equal(uint64(7), batcher.ByteSize())

	buf := &bytes.Buffer{}

	fmt.Fprintf(buf, "%+v", batcher.Batches[0])
	require.Equal("{OpType:1 Keys:[[97] [98] [99]] Values:[[49] [50] [51]] TTLs:[0 0 0]}", buf.String())
	buf.Reset()

	fmt.Fprintf(buf, "%+v", batcher.Batches[1])
	require.Equal("{OpType:2 Keys:[[97]] Values:[] TTLs:[]}", buf.String())
	buf.Reset()

	batcher.Reset()
	require.Empty(batcher.Batches)
	require.Zero(batcher.Count())
	require.Zero(batcher.ByteSize())
}

func TestTiKVSink(t *testing.T) {
	defer testleak.AfterTestT(t)()
	require := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())

	uri := "tikv://127.0.0.1:1001,127.0.0.2:1002/?concurrency=10"
	sinkURI, err := url.Parse(uri)
	require.NoError(err)

	opts := make(map[string]string)
	config, pdAddr, err := parseTiKVUri(sinkURI, opts)
	require.NoError(err)

	errCh := make(chan error)

	mockCli := &mockRawKVClient{
		b: &bytes.Buffer{},
	}

	fnCreate := func(ctx context.Context, pdAddrs []string, security tikvconfig.Security, opts ...pd.ClientOption) (rawkvClient, error) {
		return mockCli, nil
	}

	sink, err := createTiKVSink(ctx, fnCreate, config, pdAddr, opts, errCh)
	require.NoError(err)

	keys := []string{
		"a", "b", "c", "a",
	}
	values := []string{
		"1", "2", "3", "",
	}
	opTypes := []model.OpType{
		model.OpTypePut, model.OpTypePut, model.OpTypePut, model.OpTypeDelete,
	}
	for i := range keys {
		entry := &model.RawKVEntry{
			OpType: opTypes[i],
			Key:    []byte(keys[i]),
			Value:  []byte(values[i]),
			CRTs:   uint64(i),
		}
		err = sink.EmitChangedEvents(ctx, entry)
		require.NoError(err)
	}
	checkpointTs, err := sink.FlushChangedEvents(ctx, 1, uint64(120))
	require.NoError(err)
	require.Equal(uint64(120), checkpointTs)

	output := mockCli.String()
	log.Info("Output", zap.String("mockCli.String()", output))
	// c.Assert(output, check.Equals, "BatchPUT\n(a,1)(b,2)(c,3)\n")
	mockCli.Reset()

	// flush older resolved ts
	checkpointTs, err = sink.FlushChangedEvents(ctx, 1, uint64(110))
	require.NoError(err)
	require.Equal(uint64(120), checkpointTs)

	err = sink.Close(ctx)
	require.NoError(err)

	cancel()
}
