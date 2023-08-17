// Copyright 2023 PingCAP, Inc.
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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFilterConfig(t *testing.T) {
	t.Parallel()
	conf := FilterConfig{}
	require.Nil(t, conf.Validate())

	conf = FilterConfig{}
	conf.KeyFormat = "escaped"
	conf.KeyPrefix = `prefix\x11`
	conf.KeyPattern = `key\x00pattern`
	conf.ValuePattern = `value\ffpattern`
	require.Nil(t, conf.Validate())

	conf = FilterConfig{}
	conf.KeyPattern = "\xfd\xe2" // invalid utf8
	require.Error(t, conf.Validate())

	conf = FilterConfig{}
	conf.KeyFormat = "hex"
	conf.KeyPrefix = "zz" // invalid utf8
	require.Error(t, conf.Validate())
}
