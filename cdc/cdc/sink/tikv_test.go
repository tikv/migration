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

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"
	tikvconfig "github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/rawkv"
	"github.com/tikv/migration/cdc/cdc/model"
	"github.com/tikv/migration/cdc/pkg/util"
	"github.com/tikv/migration/cdc/pkg/util/testleak"
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

func TestExtractRawKVEntry(t *testing.T) {
	defer testleak.AfterTestT(t)()
	require := require.New(t)

	type expected struct {
		opType model.OpType
		key    []byte
		value  []byte
		ttl    uint64
		err    error
	}

	now := uint64(100)
	cases := []*model.RawKVEntry{
		{OpType: model.OpTypePut, Key: util.EncodeV2Key([]byte("k")), Value: []byte("v"), ExpiredTs: 0},
		{OpType: model.OpTypeDelete, Key: util.EncodeV2Key([]byte("k")), Value: []byte("v"), ExpiredTs: 0},
		{OpType: model.OpTypePut, Key: util.EncodeV2Key([]byte("k")), Value: []byte("v"), ExpiredTs: 200},
		{OpType: model.OpTypePut, Key: util.EncodeV2Key([]byte("k")), Value: []byte("v"), ExpiredTs: 100},
		{OpType: model.OpTypePut, Key: util.EncodeV2Key([]byte("k")), Value: []byte("v"), ExpiredTs: 1},
		{OpType: model.OpTypePut, Key: []byte("k"), Value: []byte("v"), ExpiredTs: 1},
	}
	expects := []expected{
		{model.OpTypePut, []byte("k"), []byte("v"), 0, nil},
		{model.OpTypeDelete, []byte("k"), nil, 0, nil},
		{model.OpTypePut, []byte("k"), []byte("v"), 100, nil},
		{model.OpTypeDelete, []byte("k"), nil, 0, nil},
		{model.OpTypeDelete, []byte("k"), nil, 0, nil},
		{model.OpTypePut, nil, nil, 0, fmt.Errorf("%s is not a valid API V2 key", []byte("k"))},
	}

	for i, c := range cases {
		opType, key, value, ttl, err := ExtractRawKVEntry(c, now)
		require.Equal(expects[i].opType, opType)
		require.Equal(expects[i].key, key)
		require.Equal(expects[i].value, value)
		require.Equal(expects[i].ttl, ttl)
		require.Equal(expects[i].err, err)
	}
}

func TestTiKVSinkConfig(t *testing.T) {
	defer testleak.AfterTestT(t)()
	require := require.New(t)

	cases := []string{
		"tikv://127.0.0.1:1001,127.0.0.2:1002,127.0.0.1:1003/?concurrency=12",
		"tikv://127.0.0.1:1001,127.0.0.1:1002/?concurrency=10&ca-path=./ca-cert.pem&cert-path=./client-cert.pem&key-path=./client-key",
	}

	expected := []struct {
		pdAddr      []string
		concurrency string
		security    tikvconfig.Security
	}{
		{[]string{"http://127.0.0.1:1001", "http://127.0.0.2:1002", "http://127.0.0.1:1003"}, "12", tikvconfig.Security{}},
		{[]string{"https://127.0.0.1:1001", "https://127.0.0.1:1002"}, "10", tikvconfig.NewSecurity("./ca-cert.pem", "./client-cert.pem", "./client-key", nil)},
	}

	for i, uri := range cases {
		sinkURI, err := url.Parse(uri)
		require.NoError(err)

		opts := make(map[string]string)
		config, pdAddr, err := ParseTiKVUri(sinkURI, opts)
		require.NoError(err)
		require.Equal(expected[i].pdAddr, pdAddr)
		require.Equal(expected[i].concurrency, opts["concurrency"])
		require.Equal(expected[i].security, config.Security)
	}
}

func TestTiKVSinkBatcher(t *testing.T) {
	defer testleak.AfterTestT(t)()
	require := require.New(t)

	fpGetNow := "github.com/tikv/migration/cdc/cdc/sink/tikvSinkGetNow"
	require.NoError(failpoint.Enable(fpGetNow, "return(100)"))
	defer func() {
		require.NoError(failpoint.Disable(fpGetNow))
	}()

	statistics := NewStatistics(context.Background(), "TiKV", map[string]string{})
	batcher := NewTiKVBatcher(statistics)
	keys := []string{
		"a", "b", "c", "d", "e", "f",
	}
	values := []string{
		"1", "2", "3", "", "5", "6",
	}
	expires := []uint64{
		0, 200, 300, 0, 100, 400,
	}
	opTypes := []model.OpType{
		model.OpTypePut, model.OpTypePut, model.OpTypePut, model.OpTypeDelete, model.OpTypePut, model.OpTypePut,
	}
	for i := range keys {
		entry0 := &model.RawKVEntry{
			OpType:    opTypes[i],
			Key:       util.EncodeV2Key([]byte(keys[i])),
			Value:     []byte(values[i]),
			ExpiredTs: expires[i],
			CRTs:      uint64(i),
		}
		// entry1 that is with invalid key will be ignored
		entry1 := &model.RawKVEntry{
			OpType:    opTypes[i],
			Key:       []byte(keys[i]),
			Value:     []byte(values[i]),
			ExpiredTs: expires[i],
			CRTs:      uint64(i),
		}
		require.NoError(batcher.Append(entry0))
		require.NoError(batcher.Append(entry1))
	}
	require.Len(batcher.Batches, 3)
	require.Equal(6, batcher.Count())
	require.Equal(42, int(batcher.ByteSize()))

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

	fpGetNow := "github.com/tikv/migration/cdc/cdc/sink/tikvSinkGetNow"
	require.NoError(failpoint.Enable(fpGetNow, "2*return(100)->return(200)"))
	defer func() {
		require.NoError(failpoint.Disable(fpGetNow))
	}()

	ctx, cancel := context.WithCancel(context.Background())

	uri := "tikv://127.0.0.1:1001,127.0.0.2:1002/?concurrency=2"
	sinkURI, err := url.Parse(uri)
	require.NoError(err)

	opts := make(map[string]string)
	config, pdAddr, err := ParseTiKVUri(sinkURI, opts)
	require.NoError(err)

	errCh := make(chan error)

	mockCli := newMockRawKVClient()

	fnCreate := func(ctx context.Context, pdAddrs []string, security tikvconfig.Security, opts ...rawkv.ClientOpt) (rawkvClient, error) {
		return mockCli, nil
	}

	sink, err := createTiKVSink(ctx, fnCreate, config, pdAddr, opts, errCh)
	require.NoError(err)

	// Batch 0
	{
		keys := []string{
			"a", "b", "c", "d", "e", "f",
		}
		values := []string{
			"1", "2", "3", "", "5", "6",
		}
		expires := []uint64{
			0, 200, 300, 0, 100, 400,
		}
		opTypes := []model.OpType{
			model.OpTypePut, model.OpTypePut, model.OpTypePut, model.OpTypeDelete, model.OpTypePut, model.OpTypePut,
		}
		for i := range keys {
			entry := &model.RawKVEntry{
				OpType:    opTypes[i],
				Key:       util.EncodeV2Key([]byte(keys[i])),
				Value:     []byte(values[i]),
				ExpiredTs: expires[i],
				CRTs:      uint64(i),
			}
			err = sink.EmitChangedEvents(ctx, entry)
			require.NoError(err)
		}
	}
	checkpointTs, err := sink.FlushChangedEvents(ctx, 1, uint64(120))
	require.NoError(err)
	require.EqualValues(120, checkpointTs)

	// Batch 1 with now:200
	{
		keys := []string{
			"k", "m", "n",
		}
		values := []string{
			"1", "2", "3",
		}
		expires := []uint64{
			0, 200, 300,
		}
		opTypes := []model.OpType{
			model.OpTypePut, model.OpTypePut, model.OpTypePut,
		}
		for i := range keys {
			entry := &model.RawKVEntry{
				OpType:    opTypes[i],
				Key:       util.EncodeV2Key([]byte(keys[i])),
				Value:     []byte(values[i]),
				ExpiredTs: expires[i],
				CRTs:      uint64(i),
			}
			err = sink.EmitChangedEvents(ctx, entry)
			require.NoError(err)
		}
	}
	checkpointTs, err = sink.FlushChangedEvents(ctx, 1, uint64(140))
	require.NoError(err)
	require.EqualValues(140, checkpointTs)

	// Check result
	mockCli.CloseOutput()
	results := make([]string, 0)
	for r := range mockCli.Output() {
		results = append(results, r)
	}
	sort.Slice(results, func(i, j int) bool { return results[i] < results[j] })
	require.Equal([]string{"D:d|", "D:e|", "D:m|", "P:a,1,0|", "P:b,2,100|P:c,3,200|", "P:f,6,300|", "P:k,1,0|", "P:n,3,100|"}, results)

	// Flush older resolved ts
	checkpointTs, err = sink.FlushChangedEvents(ctx, 1, uint64(110))
	require.NoError(err)
	require.EqualValues(140, checkpointTs)

	err = sink.Close(ctx)
	require.NoError(err)

	cancel()
}
