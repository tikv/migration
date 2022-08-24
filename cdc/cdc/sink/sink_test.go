// Copyright 2021 PingCAP, Inc.
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
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tikv/migration/cdc/pkg/config"
	"github.com/tikv/migration/cdc/pkg/util/testleak"
)

func TestValidateSink(t *testing.T) {
	defer testleak.AfterTestT(t)()

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	replicateConfig := config.GetDefaultReplicaConfig()
	opts := make(map[string]string)

	testCases := []struct {
		sinkURI  string
		expected bool
	}{
		{"tikv://127.0.0.1:3306/", true},
		{"tikv://127.0.0.1:3306/?concurrency=4", true},
		{"blackhole://", true},
		{"tikv://127.0.0.1:3306,127.0.0.1:3307/", true},
		{"tikv://hostname:3306", true},
		{"tikv://http://127.0.0.1:3306/", false},
		{"tikv://127.0.0.1:3306a/", false},
		{"tikv://127.0.0.1:3306, tikv://127.0.0.1:3307/", false},
		{"tikv://hostname:3306x", false},
	}

	for _, tc := range testCases {
		err := Validate(ctx, tc.sinkURI, replicateConfig, opts)
		require.Equal(t, tc.expected, err == nil)
	}
}
