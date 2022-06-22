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
// See the License for the specific language governing permissions and
// limitations under the License.

package sink

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	tikvconfig "github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/rawkv"
	"github.com/tikv/migration/cdc/cdc/model"
	"github.com/tikv/migration/cdc/pkg/util/testleak"
	pd "github.com/tikv/pd/client"
)

type mockRawKVClient struct {
	ch chan string
}

func newMockRawKVClient() *mockRawKVClient {
	return &mockRawKVClient{
		ch: make(chan string, 1024),
	}
}

func (c *mockRawKVClient) Output() <-chan string {
	return c.ch
}

func (c *mockRawKVClient) CloseOutput() {
	close(c.ch)
}

func (c *mockRawKVClient) BatchPutWithTTL(ctx context.Context, keys, values [][]byte, ttls []uint64, options ...rawkv.RawOption) error {
	b := &bytes.Buffer{}
	for i, k := range keys {
		fmt.Fprintf(b, "P:%s,%s,%d|", string(k), string(values[i]), ttls[i])
	}
	c.ch <- b.String()
	return nil
}

func (c *mockRawKVClient) BatchDelete(ctx context.Context, keys [][]byte, options ...rawkv.RawOption) error {
	b := &bytes.Buffer{}
	for _, k := range keys {
		fmt.Fprintf(b, "D:%s|", string(k))
	}
	c.ch <- b.String()
	return nil
}

func (c *mockRawKVClient) Close() error {
	return nil
}

var _ rawkvClient = (*mockRawKVClient)(nil)

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

	getNow = func() uint64 {
		return 100
	}

	batcher := tikvBatcher{}
	keys := []string{
		"a", "b", "c", "d", "e", "f",
	}
	values := []string{
		"1", "2", "3", "", "5", "6",
	}
	expireds := []uint64{
		0, 200, 300, 0, 100, 400,
	}
	opTypes := []model.OpType{
		model.OpTypePut, model.OpTypePut, model.OpTypePut, model.OpTypeDelete, model.OpTypePut, model.OpTypePut,
	}
	for i := range keys {
		entry := &model.RawKVEntry{
			OpType:    opTypes[i],
			Key:       []byte(keys[i]),
			Value:     []byte(values[i]),
			ExpiredTs: expireds[i],
			CRTs:      uint64(i),
		}
		batcher.Append(entry)
	}
	require.Len(batcher.Batches, 3)
	require.Equal(6, batcher.Count())
	require.Equal(10, int(batcher.ByteSize()))

	buf := &bytes.Buffer{}

	for _, batch := range batcher.Batches {
		fmt.Fprintf(buf, "%+v\n", batch)
	}
	require.Equal(`{OpType:1 Keys:[[97] [98] [99]] Values:[[49] [50] [51]] TTLs:[0 100 200]}
{OpType:2 Keys:[[100] [101]] Values:[] TTLs:[]}
{OpType:1 Keys:[[102]] Values:[[54]] TTLs:[300]}
`, buf.String())

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

	mockCli := newMockRawKVClient()

	fnCreate := func(ctx context.Context, pdAddrs []string, security tikvconfig.Security, opts ...pd.ClientOption) (rawkvClient, error) {
		return mockCli, nil
	}

	sink, err := createTiKVSink(ctx, fnCreate, config, pdAddr, opts, errCh)
	require.NoError(err)

	keys := []string{
		"a", "b", "c", "d", "e", "f",
	}
	values := []string{
		"1", "2", "3", "", "5", "6",
	}
	expireds := []uint64{
		0, 200, 300, 0, 100, 400,
	}
	opTypes := []model.OpType{
		model.OpTypePut, model.OpTypePut, model.OpTypePut, model.OpTypeDelete, model.OpTypePut, model.OpTypePut,
	}
	for i := range keys {
		entry := &model.RawKVEntry{
			OpType:    opTypes[i],
			Key:       []byte(keys[i]),
			Value:     []byte(values[i]),
			ExpiredTs: expireds[i],
			CRTs:      uint64(i),
		}
		err = sink.EmitChangedEvents(ctx, entry)
		require.NoError(err)
	}
	checkpointTs, err := sink.FlushChangedEvents(ctx, 1, uint64(120))
	require.NoError(err)
	require.Equal(uint64(120), checkpointTs)

	mockCli.CloseOutput()
	results := make([]string, 0)
	for r := range mockCli.Output() {
		results = append(results, r)
	}
	sort.Slice(results, func(i, j int) bool { return results[i] < results[j] })
	require.Equal([]string{"D:d|D:e|", "P:a,1,0|", "P:b,2,100|", "P:c,3,200|", "P:f,6,300|"}, results)

	// flush older resolved ts
	checkpointTs, err = sink.FlushChangedEvents(ctx, 1, uint64(110))
	require.NoError(err)
	require.Equal(uint64(120), checkpointTs)

	err = sink.Close(ctx)
	require.NoError(err)

	cancel()
}
