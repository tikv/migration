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

	"github.com/pkg/errors"
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

	expectedErrs := []string{
		"[pd] failed to get cluster id",
		"Invalid pd addr: http:, err: <nil>",
		"parse \"tikv://127.0.0.1:3306a/\": invalid port \":3306a\" after host",
		"parse \"tikv://127.0.0.1:3306, tikv://127.0.0.1:3307/\": invalid character \" \" in host name",
		"parse \"tikv://hostname:3306x\": invalid port \":3306x\" after host",
	}

	testCases := []struct {
		sinkURI     string
		hasError    bool
		expectedErr string
	}{
		{"tikv://127.0.0.1:3306/", true, expectedErrs[0]},
		{"tikv://127.0.0.1:3306/?concurrency=4", true, expectedErrs[0]},
		{"blackhole://", false, ""},
		{"tikv://127.0.0.1:3306,127.0.0.1:3307/", true, expectedErrs[0]},
		{"tikv://hostname:3306", true, expectedErrs[0]},
		{"tikv://http://127.0.0.1:3306/", true, expectedErrs[1]},
		{"tikv://127.0.0.1:3306a/", true, expectedErrs[2]},
		{"tikv://127.0.0.1:3306, tikv://127.0.0.1:3307/", true, expectedErrs[3]},
		{"tikv://hostname:3306x", true, expectedErrs[4]},
	}

	for _, tc := range testCases {
		err := Validate(ctx, tc.sinkURI, replicateConfig, opts)
		if tc.hasError {
			require.Equal(t, tc.expectedErr, errors.Cause(err).Error())
		} else {
			require.NoError(t, err)
		}
	}
}
