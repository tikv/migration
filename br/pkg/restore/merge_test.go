// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util/codec"
	berrors "github.com/tikv/migration/br/pkg/errors"
)

type fileBulder struct {
	startKeyOffset int64
}

func (fb *fileBulder) build(num, bytes, kv int) (files []*backuppb.File) {
	if num != 1 && num != 2 {
		panic("num must be 1 or 2")
	}

	fb.startKeyOffset += 10

	startKey := codec.EncodeInt(nil, fb.startKeyOffset)
	fb.startKeyOffset += 10
	endKey := codec.EncodeInt(nil, fb.startKeyOffset)
	files = append(files, &backuppb.File{
		Name:       fmt.Sprint(rand.Int63n(math.MaxInt64), "_write.sst"),
		StartKey:   startKey,
		EndKey:     endKey,
		TotalKvs:   uint64(kv),
		TotalBytes: uint64(bytes),
		Cf:         "write",
	})
	if num == 1 {
		return
	}

	// To match TiKV's behavior.
	files[0].TotalKvs = 0
	files[0].TotalBytes = 0
	files = append(files, &backuppb.File{
		Name:       fmt.Sprint(rand.Int63n(math.MaxInt64), "_default.sst"),
		StartKey:   startKey,
		EndKey:     endKey,
		TotalKvs:   uint64(kv),
		TotalBytes: uint64(bytes),
		Cf:         "default",
	})
	return files
}

func TestMergeRanges(t *testing.T) {
	type Case struct {
		files  [][5]int // tableID, indexID num, bytes, kv
		merged []int    // length of each merged range
		stat   MergeRangesStat
	}
	splitSizeBytes := int(DefaultMergeRegionSizeBytes)
	splitKeyCount := int(DefaultMergeRegionKeyCount)
	cases := []Case{
		// Empty backup.
		{
			files:  [][5]int{},
			merged: []int{},
			stat:   MergeRangesStat{TotalRegions: 0, MergedRegions: 0},
		},

		// Do not merge big range.
		{
			files:  [][5]int{{1, 0, 1, splitSizeBytes, 1}, {1, 0, 1, 1, 1}},
			merged: []int{1, 1},
			stat:   MergeRangesStat{TotalRegions: 2, MergedRegions: 2},
		},
		{
			files:  [][5]int{{1, 0, 1, 1, 1}, {1, 0, 1, splitSizeBytes, 1}},
			merged: []int{1, 1},
			stat:   MergeRangesStat{TotalRegions: 2, MergedRegions: 2},
		},
		{
			files:  [][5]int{{1, 0, 1, 1, splitKeyCount}, {1, 0, 1, 1, 1}},
			merged: []int{1, 1},
			stat:   MergeRangesStat{TotalRegions: 2, MergedRegions: 2},
		},
		{
			files:  [][5]int{{1, 0, 1, 1, 1}, {1, 0, 1, 1, splitKeyCount}},
			merged: []int{1, 1},
			stat:   MergeRangesStat{TotalRegions: 2, MergedRegions: 2},
		},

		// 3 -> 1
		{
			files:  [][5]int{{1, 0, 1, 1, 1}, {1, 0, 1, 1, 1}, {1, 0, 1, 1, 1}},
			merged: []int{3},
			stat:   MergeRangesStat{TotalRegions: 3, MergedRegions: 1},
		},
		// 3 -> 2, size: [split*1/3, split*1/3, split*1/2] -> [split*2/3, split*1/2]
		{
			files:  [][5]int{{1, 0, 1, splitSizeBytes / 3, 1}, {1, 0, 1, splitSizeBytes / 3, 1}, {1, 0, 1, splitSizeBytes / 2, 1}},
			merged: []int{2, 1},
			stat:   MergeRangesStat{TotalRegions: 3, MergedRegions: 2},
		},
		// 4 -> 2, size: [split*1/3, split*1/3, split*1/2, 1] -> [split*2/3, split*1/2 +1]
		{
			files:  [][5]int{{1, 0, 1, splitSizeBytes / 3, 1}, {1, 0, 1, splitSizeBytes / 3, 1}, {1, 0, 1, splitSizeBytes / 2, 1}, {1, 0, 1, 1, 1}},
			merged: []int{2, 2},
			stat:   MergeRangesStat{TotalRegions: 4, MergedRegions: 2},
		},
		// 5 -> 3, size: [split*1/3, split*1/3, split, split*1/2, 1] -> [split*2/3, split, split*1/2 +1]
		{
			files:  [][5]int{{1, 0, 1, splitSizeBytes / 3, 1}, {1, 0, 1, splitSizeBytes / 3, 1}, {1, 0, 1, splitSizeBytes, 1}, {1, 0, 1, splitSizeBytes / 2, 1}, {1, 0, 1, 1, 1}},
			merged: []int{2, 1, 2},
			stat:   MergeRangesStat{TotalRegions: 5, MergedRegions: 3},
		},
	}

	for i, cs := range cases {
		files := make([]*backuppb.File, 0)
		fb := fileBulder{}
		for _, f := range cs.files {
			files = append(files, fb.build(f[2], f[3], f[4])...)
		}
		rngs, stat, err := MergeFileRanges(files, DefaultMergeRegionSizeBytes, DefaultMergeRegionKeyCount)
		require.NoErrorf(t, err, "%+v", cs)
		require.Equalf(t, cs.stat.TotalRegions, stat.TotalRegions, "%+v", cs)
		require.Equalf(t, cs.stat.MergedRegions, stat.MergedRegions, "%+v", cs)
		require.Lenf(t, rngs, len(cs.merged), "case %d", i)
		for i, rg := range rngs {
			require.Lenf(t, rg.Files, cs.merged[i], "%+v", cs)
			// Files range must be in [Range.StartKey, Range.EndKey].
			for _, f := range rg.Files {
				require.LessOrEqual(t, bytes.Compare(rg.StartKey, f.StartKey), 0)
				require.GreaterOrEqual(t, bytes.Compare(rg.EndKey, f.EndKey), 0)
			}
		}
	}
}

func TestMergeRawKVRanges(t *testing.T) {
	files := make([]*backuppb.File, 0)
	fb := fileBulder{}
	files = append(files, fb.build(2, 1, 1)...)
	// RawKV does not have write cf
	files = files[1:]
	_, stat, err := MergeFileRanges(
		files, DefaultMergeRegionSizeBytes, DefaultMergeRegionKeyCount)
	require.NoError(t, err)
	require.Equal(t, 1, stat.TotalRegions)
	require.Equal(t, 1, stat.MergedRegions)
}

func TestInvalidRanges(t *testing.T) {
	files := make([]*backuppb.File, 0)
	fb := fileBulder{}
	files = append(files, fb.build(1, 1, 1)...)
	files[0].Name = "invalid.sst"
	files[0].Cf = "invalid"
	_, _, err := MergeFileRanges(
		files, DefaultMergeRegionSizeBytes, DefaultMergeRegionKeyCount)
	require.Error(t, err)
	require.Equal(t, berrors.ErrRestoreInvalidBackup, errors.Cause(err))
}

// Benchmark results on Intel(R) Xeon(R) CPU E5-2630 v4 @ 2.20GHz
//
// BenchmarkMergeRanges100-40          9676             114344 ns/op
// BenchmarkMergeRanges1k-40            345            3700739 ns/op
// BenchmarkMergeRanges10k-40             3          414097277 ns/op
// BenchmarkMergeRanges50k-40             1        17258177908 ns/op
// BenchmarkMergeRanges100k-40            1        73403873161 ns/op

func benchmarkMergeRanges(b *testing.B, filesCount int) {
	files := make([]*backuppb.File, 0)
	fb := fileBulder{}
	for i := 0; i < filesCount; i++ {
		files = append(files, fb.build(1, 1, 1)...)
	}
	var err error
	for i := 0; i < b.N; i++ {
		_, _, err = MergeFileRanges(files, DefaultMergeRegionSizeBytes, DefaultMergeRegionKeyCount)
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkMergeRanges100(b *testing.B) {
	benchmarkMergeRanges(b, 100)
}

func BenchmarkMergeRanges1k(b *testing.B) {
	benchmarkMergeRanges(b, 1000)
}

func BenchmarkMergeRanges10k(b *testing.B) {
	benchmarkMergeRanges(b, 10000)
}

func BenchmarkMergeRanges50k(b *testing.B) {
	benchmarkMergeRanges(b, 50000)
}

func BenchmarkMergeRanges100k(b *testing.B) {
	benchmarkMergeRanges(b, 100000)
}
