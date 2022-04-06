// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"bytes"
	"context"
	"strings"

	_ "github.com/go-sql-driver/mysql" // mysql driver
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	berrors "github.com/tikv/migration/br/pkg/errors"
	"github.com/tikv/migration/br/pkg/glue"
	"github.com/tikv/migration/br/pkg/logutil"
	"github.com/tikv/migration/br/pkg/rtree"
	"go.uber.org/zap"
)

// GetSSTMetaFromFile compares the keys in file, region and rewrite rules, then returns a sst conn.
// The range of the returned sst meta is [regionRule.NewKeyPrefix, append(regionRule.NewKeyPrefix, 0xff)].
func GetSSTMetaFromFile(
	id []byte,
	file *backuppb.File,
	region *metapb.Region,
	regionRule *import_sstpb.RewriteRule,
) import_sstpb.SSTMeta {
	// Get the column family of the file by the file name.
	var cfName string
	if strings.Contains(file.GetName(), defaultCFName) {
		cfName = defaultCFName
	} else if strings.Contains(file.GetName(), writeCFName) {
		cfName = writeCFName
	}
	// Find the overlapped part between the file and the region.
	// Here we rewrites the keys to compare with the keys of the region.
	rangeStart := regionRule.GetNewKeyPrefix()
	//  rangeStart = max(rangeStart, region.StartKey)
	if bytes.Compare(rangeStart, region.GetStartKey()) < 0 {
		rangeStart = region.GetStartKey()
	}

	// Append 10 * 0xff to make sure rangeEnd cover all file key
	// If choose to regionRule.NewKeyPrefix + 1, it may cause WrongPrefix here
	// https://github.com/tikv/tikv/blob/970a9bf2a9ea782a455ae579ad237aaf6cb1daec/
	// components/sst_importer/src/sst_importer.rs#L221
	suffix := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	rangeEnd := append(append([]byte{}, regionRule.GetNewKeyPrefix()...), suffix...)
	// rangeEnd = min(rangeEnd, region.EndKey)
	if len(region.GetEndKey()) > 0 && bytes.Compare(rangeEnd, region.GetEndKey()) > 0 {
		rangeEnd = region.GetEndKey()
	}

	if bytes.Compare(rangeStart, rangeEnd) > 0 {
		log.Panic("range start exceed range end",
			logutil.File(file),
			logutil.Key("startKey", rangeStart),
			logutil.Key("endKey", rangeEnd))
	}

	log.Debug("get sstMeta",
		logutil.File(file),
		logutil.Key("startKey", rangeStart),
		logutil.Key("endKey", rangeEnd))

	return import_sstpb.SSTMeta{
		Uuid:   id,
		CfName: cfName,
		Range: &import_sstpb.Range{
			Start: rangeStart,
			End:   rangeEnd,
		},
		Length:      file.GetSize_(),
		RegionId:    region.GetId(),
		RegionEpoch: region.GetRegionEpoch(),
		CipherIv:    file.GetCipherIv(),
	}
}

// Rewrites a raw key and returns a encoded key.
func rewriteRawKey(key []byte, rewriteRules *RewriteRules) ([]byte, *import_sstpb.RewriteRule) {
	if rewriteRules == nil {
		return codec.EncodeBytes([]byte{}, key), nil
	}
	if len(key) > 0 {
		rule := matchOldPrefix(key, rewriteRules)
		ret := bytes.Replace(key, rule.GetOldKeyPrefix(), rule.GetNewKeyPrefix(), 1)
		return codec.EncodeBytes([]byte{}, ret), rule
	}
	return nil, nil
}

func matchOldPrefix(key []byte, rewriteRules *RewriteRules) *import_sstpb.RewriteRule {
	for _, rule := range rewriteRules.Data {
		if bytes.HasPrefix(key, rule.GetOldKeyPrefix()) {
			return rule
		}
	}
	return nil
}

// SplitRanges splits region by
// 1. data range after rewrite.
// 2. rewrite rules.
func SplitRanges(
	ctx context.Context,
	client *Client,
	ranges []rtree.Range,
	rewriteRules *RewriteRules,
	updateCh glue.Progress,
) error {
	splitter := NewRegionSplitter(NewSplitClient(client.GetPDClient(), client.GetTLSConfig()))

	return splitter.Split(ctx, ranges, rewriteRules, func(keys [][]byte) {
		for range keys {
			updateCh.Inc()
		}
	})
}

func rewriteFileKeys(file *backuppb.File, rewriteRules *RewriteRules) (startKey, endKey []byte, err error) {
	startID := tablecodec.DecodeTableID(file.GetStartKey())
	endID := tablecodec.DecodeTableID(file.GetEndKey())
	var rule *import_sstpb.RewriteRule
	if startID == endID {
		startKey, rule = rewriteRawKey(file.GetStartKey(), rewriteRules)
		if rewriteRules != nil && rule == nil {
			log.Error("cannot find rewrite rule",
				logutil.Key("startKey", file.GetStartKey()),
				zap.Reflect("rewrite data", rewriteRules.Data))
			err = errors.Annotate(berrors.ErrRestoreInvalidRewrite, "cannot find rewrite rule for start key")
			return
		}
		endKey, rule = rewriteRawKey(file.GetEndKey(), rewriteRules)
		if rewriteRules != nil && rule == nil {
			err = errors.Annotate(berrors.ErrRestoreInvalidRewrite, "cannot find rewrite rule for end key")
			return
		}
	} else {
		log.Error("table ids dont matched",
			zap.Int64("startID", startID),
			zap.Int64("endID", endID),
			logutil.Key("startKey", startKey),
			logutil.Key("endKey", endKey))
		err = errors.Annotate(berrors.ErrRestoreInvalidRewrite, "invalid table id")
	}
	return
}
