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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gluetikv

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGlue(t *testing.T) {
	g := Glue{}
	require.Regexp(t, "^BR(.|\n)*Release Version(.|\n)*Git Commit Hash(.|\n)*$", g.GetVersion())

	require.True(t, g.OwnsStorage())

	ctx := context.Background()
	steps := int64(10)
	updatCh := g.StartProgress(ctx, "test", steps, false)
	for i := int64(0); i < steps; i++ {
		updatCh.Inc()
		g.Record("backup", 10)
	}
	updatCh.Close()
}
