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

package util

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

type ctxKey string

const (
	ctxKeyKeySpanID    = ctxKey("keyspanID")
	ctxKeyCaptureAddr  = ctxKey("captureAddr")
	ctxKeyChangefeedID = ctxKey("changefeedID")
	ctxKeyIsOwner      = ctxKey("isOwner")
	ctxKeyTimezone     = ctxKey("timezone")
	ctxKeyKVStorage    = ctxKey("kvStorage")
	ctxEventFilter     = ctxKey("eventFilter")
)

// CaptureAddrFromCtx returns a capture ID stored in the specified context.
// It returns an empty string if there's no valid capture ID found.
func CaptureAddrFromCtx(ctx context.Context) string {
	captureAddr, ok := ctx.Value(ctxKeyCaptureAddr).(string)
	if !ok {
		return ""
	}
	return captureAddr
}

// PutCaptureAddrInCtx returns a new child context with the specified capture ID stored.
func PutCaptureAddrInCtx(ctx context.Context, captureAddr string) context.Context {
	return context.WithValue(ctx, ctxKeyCaptureAddr, captureAddr)
}

// PutTimezoneInCtx returns a new child context with the given timezone
func PutTimezoneInCtx(ctx context.Context, timezone *time.Location) context.Context {
	return context.WithValue(ctx, ctxKeyTimezone, timezone)
}

// PutKVStorageInCtx returns a new child context with the given tikv store
func PutKVStorageInCtx(ctx context.Context, store tikv.Storage) context.Context {
	return context.WithValue(ctx, ctxKeyKVStorage, store)
}

type keyspaninfo struct {
	id   uint64
	name string
}

// PutKeySpanInfoInCtx returns a new child context with the specified keyspan ID and name stored.
func PutKeySpanInfoInCtx(ctx context.Context, keyspanID uint64, keyspanName string) context.Context {
	return context.WithValue(ctx, ctxKeyKeySpanID, keyspaninfo{id: keyspanID, name: keyspanName})
}

// KeySpanInfoFromCtx returns a kyspan ID & name
func KeySpanInfoFromCtx(ctx context.Context) (uint64, string) {
	info, ok := ctx.Value(ctxKeyKeySpanID).(keyspaninfo)
	if !ok {
		return 0, ""
	}
	return info.id, info.name
}

// TimezoneFromCtx returns a timezone
func TimezoneFromCtx(ctx context.Context) *time.Location {
	tz, ok := ctx.Value(ctxKeyTimezone).(*time.Location)
	if !ok {
		return nil
	}
	return tz
}

// KVStorageFromCtx returns a tikv store
func KVStorageFromCtx(ctx context.Context) (tikv.Storage, error) {
	store, ok := ctx.Value(ctxKeyKVStorage).(tikv.Storage)
	if !ok {
		return nil, errors.Errorf("context can not find the value associated with key: %s", ctxKeyKVStorage)
	}
	return store, nil
}

// SetOwnerInCtx returns a new child context with the owner flag set.
func SetOwnerInCtx(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxKeyIsOwner, true)
}

// IsOwnerFromCtx returns true if this capture is owner
func IsOwnerFromCtx(ctx context.Context) bool {
	isOwner := ctx.Value(ctxKeyIsOwner)
	return isOwner != nil && isOwner.(bool)
}

// ChangefeedIDFromCtx returns a changefeedID stored in the specified context.
// It returns an empty string if there's no valid changefeed ID found.
func ChangefeedIDFromCtx(ctx context.Context) string {
	changefeedID, ok := ctx.Value(ctxKeyChangefeedID).(string)
	if !ok {
		return ""
	}
	return changefeedID
}

// PutChangefeedIDInCtx returns a new child context with the specified changefeed ID stored.
func PutChangefeedIDInCtx(ctx context.Context, changefeedID string) context.Context {
	return context.WithValue(ctx, ctxKeyChangefeedID, changefeedID)
}

func EventFilterFromCtx(ctx context.Context) *KvFilter {
	filter, ok := ctx.Value(ctxEventFilter).(*KvFilter)
	if !ok {
		return nil
	}
	return filter
}

func PutEventFilterInCtx(ctx context.Context, filter *KvFilter) context.Context {
	return context.WithValue(ctx, ctxEventFilter, filter)
}

// ZapFieldCapture returns a zap field containing capture address
// TODO: log redact for capture address
func ZapFieldCapture(ctx context.Context) zap.Field {
	return zap.String("capture", CaptureAddrFromCtx(ctx))
}

// ZapFieldChangefeed returns a zap field containing changefeed id
func ZapFieldChangefeed(ctx context.Context) zap.Field {
	return zap.String("changefeed", ChangefeedIDFromCtx(ctx))
}
