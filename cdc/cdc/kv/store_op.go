// Copyright 2020 PingCAP, Inc.
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

package kv

import (
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	tikvconfig "github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/migration/cdc/cdc/model"
	cerror "github.com/tikv/migration/cdc/pkg/errors"
	"github.com/tikv/migration/cdc/pkg/flags"
	"github.com/tikv/migration/cdc/pkg/security"
	"go.uber.org/zap"
)

// TiKVStorage is the tikv storage interface used by CDC.
type TiKVStorage interface {
	tikv.Storage
	GetCachedCurrentVersion() (version uint64, err error)
}

const (
	storageVersionCacheUpdateInterval = time.Second * 2
)

// StorageWithCurVersionCache adds GetCachedCurrentVersion() to tikv.Storage
type StorageWithCurVersionCache struct {
	tikv.Storage
	cacheKey string
}

type curVersionCacheEntry struct {
	ts          model.Ts
	lastUpdated time.Time
	mu          sync.Mutex
}

var (
	curVersionCache   = make(map[string]*curVersionCacheEntry, 1)
	curVersionCacheMu sync.Mutex
)

func newStorageWithCurVersionCache(storage tikv.Storage, cacheKey string) TiKVStorage {
	curVersionCacheMu.Lock()
	defer curVersionCacheMu.Unlock()

	if _, exists := curVersionCache[cacheKey]; !exists {
		curVersionCache[cacheKey] = &curVersionCacheEntry{
			ts:          0,
			lastUpdated: time.Unix(0, 0),
		}
	}

	return &StorageWithCurVersionCache{
		Storage:  storage,
		cacheKey: cacheKey,
	}
}

// GetCachedCurrentVersion gets the cached version of currentVersion, and update the cache if necessary
func (s *StorageWithCurVersionCache) GetCachedCurrentVersion() (version uint64, err error) {
	curVersionCacheMu.Lock()
	entry, exists := curVersionCache[s.cacheKey]
	curVersionCacheMu.Unlock()

	if !exists {
		err = cerror.ErrCachedTSONotExists.GenWithStackByArgs()
		log.Warn("GetCachedCurrentVersion: cache entry does not exist", zap.String("cacheKey", s.cacheKey))
		return
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()

	if time.Now().After(entry.lastUpdated.Add(storageVersionCacheUpdateInterval)) {
		var ts uint64
		ts, err = s.CurrentTimestamp(oracle.GlobalTxnScope)
		if err != nil {
			return
		}
		entry.ts = ts
		entry.lastUpdated = time.Now()
	}

	version = entry.ts
	return
}

// CreateTiStore creates a new tikv storage client
func CreateTiStore(urls string, credential *security.Credential) (tikv.Storage, error) {
	urlv, err := flags.NewURLsValue(urls)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if credential.CAPath != "" {
		conf := tikvconfig.GetGlobalConfig()
		conf.Security.ClusterSSLCA = credential.CAPath
		conf.Security.ClusterSSLCert = credential.CertPath
		conf.Security.ClusterSSLKey = credential.KeyPath
		tikvconfig.StoreGlobalConfig(conf)
	}

	tiStore, err := txnkv.NewClient(urlv.HostList())
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrNewStore, err)
	}
	return tiStore, nil
}
