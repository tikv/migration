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

package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/check"
	"github.com/spf13/cobra"
	"github.com/tikv/migration/cdc/pkg/config"
	"github.com/tikv/migration/cdc/pkg/util/testleak"
)

func TestChangefeedSuite(t *testing.T) { check.TestingT(t) }

type changefeedSuite struct{}

var _ = check.Suite(&changefeedSuite{})

func (s *changefeedSuite) TestStrictDecodeConfig(c *check.C) {
	defer testleak.AfterTest(c)()
	cmd := new(cobra.Command)
	o := newChangefeedCommonOptions()
	o.addFlags(cmd)

	dir := c.MkDir()
	path := filepath.Join(dir, "config.toml")
	content := `
	check-gc-safe-point = true
	[filter]
	key-prefix = "key\\x00"
	key-pattern = "key\\x00pattern"
	value-pattern = "value\\ffpattern"
	`
	err := os.WriteFile(path, []byte(content), 0o644)
	c.Assert(err, check.IsNil)

	c.Assert(cmd.ParseFlags([]string{fmt.Sprintf("--config=%s", path)}), check.IsNil)

	cfg := config.GetDefaultReplicaConfig()
	err = o.strictDecodeConfig("cdc", cfg)
	c.Assert(err, check.IsNil)
	c.Assert(cfg.CheckGCSafePoint, check.IsTrue)
	c.Assert(cfg.Filter.KeyPrefix, check.Equals, `key\x00`)
	c.Assert(cfg.Filter.KeyPattern, check.Equals, `key\x00pattern`)
	c.Assert(cfg.Filter.ValuePattern, check.Equals, `value\ffpattern`)

	path = filepath.Join(dir, "config1.toml")
	content = `
	check-gc-safe-point? = true`
	err = os.WriteFile(path, []byte(content), 0o644)
	c.Assert(err, check.IsNil)

	c.Assert(cmd.ParseFlags([]string{fmt.Sprintf("--config=%s", path)}), check.IsNil)

	cfg = config.GetDefaultReplicaConfig()
	err = o.strictDecodeConfig("cdc", cfg)
	c.Assert(err, check.NotNil)
	c.Assert(err, check.ErrorMatches, "toml: .*")
}

type TestKeyRange struct {
	Format   string
	StartKey string
	EndKey   string
	Valid    bool
}

func (s *changefeedSuite) TestValidateKeyFormat(c *check.C) {
	defer testleak.AfterTest(c)()
	o := newChangefeedCommonOptions()
	testCases := []TestKeyRange{
		{
			Format:   "hex",
			StartKey: "",
			EndKey:   "",
			Valid:    true,
		},
		{
			Format:   "hex",
			StartKey: "abef5612",
			EndKey:   "bef34689",
			Valid:    true,
		},
		{
			Format:   "abc", // supported format
			StartKey: "",
			EndKey:   "",
			Valid:    false,
		},
		{
			Format:   "hex",
			StartKey: "7889efac",
			EndKey:   "zx89efac", // wrong format
			Valid:    false,
		},
		{
			Format:   "hex",
			StartKey: "5672acba",
			EndKey:   "5672acaa", // end is smaller than start
			Valid:    false,
		},
		{
			Format:   "raw",
			StartKey: "",
			EndKey:   "",
			Valid:    true,
		},
		{
			Format:   "raw",
			StartKey: "abc",
			EndKey:   "def",
			Valid:    true,
		},
		{
			Format:   "escaped",
			StartKey: "\\a\\x1",
			EndKey:   "\\b\\f",
			Valid:    true,
		},
	}
	for _, testKeyRange := range testCases {
		o.startKey = testKeyRange.StartKey
		o.endKey = testKeyRange.EndKey
		o.format = testKeyRange.Format
		if testKeyRange.Valid {
			c.Assert(o.validKeyFormat(), check.IsNil)
		} else {
			c.Assert(o.validKeyFormat(), check.NotNil)
		}
	}
}
