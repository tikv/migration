package backup

import (
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/migration/br/pkg/rtree"
)

type testRangeTreeItem struct {
	start []byte
	end   []byte
	file  []*backuppb.File
}

func TestCheckDupFile(t *testing.T) {
	files := []testRangeTreeItem{
		{
			start: []byte("a"),
			end:   []byte("b"),
			file: []*backuppb.File{
				{
					Name: "range-a-b",
				},
			},
		},
		{
			start: []byte("b"),
			end:   []byte("c"),
			file: []*backuppb.File{
				{
					Name: "range-b-c",
				},
			},
		},
	}
	rgTree := rtree.NewRangeTree()
	for _, item := range files {
		rgTree.Put(item.start, item.end, item.file)
	}
	hasDup := checkDupFiles(&rgTree)
	require.False(t, hasDup)
	dedupFileItem := testRangeTreeItem{
		start: []byte("c"),
		end:   []byte("d"),
		file: []*backuppb.File{
			{
				Name: "range-a-b",
			},
		},
	}
	rgTree.Put(dedupFileItem.start, dedupFileItem.end, dedupFileItem.file)
	hasDup = checkDupFiles(&rgTree)
	require.True(t, hasDup)
}
