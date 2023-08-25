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
	"strings"

	"github.com/pingcap/check"
	"github.com/pingcap/log"
	"github.com/tikv/migration/cdc/cdc/model"
	"github.com/tikv/migration/cdc/pkg/util/testleak"
)

type changefeedUpdateSuite struct{}

var _ = check.Suite(&changefeedUpdateSuite{})

func (s *changefeedUpdateSuite) TestApplyChanges(c *check.C) {
	defer testleak.AfterTest(c)()

	cmd := NewCmdCli()
	commonChangefeedOptions := newChangefeedCommonOptions()
	o := newUpdateChangefeedOptions(commonChangefeedOptions)
	o.addFlags(cmd)

	// Test normal update.
	oldInfo := &model.ChangeFeedInfo{SinkURI: "blackhole://"}
	c.Assert(cmd.ParseFlags([]string{"--sink-uri=mysql://root@downstream-tidb:4000"}), check.IsNil)
	newInfo, err := o.applyChanges(oldInfo, cmd)
	c.Assert(err, check.IsNil)
	c.Assert(newInfo.SinkURI, check.Equals, "mysql://root@downstream-tidb:4000")

	// Test update config file
	oldInfo = &model.ChangeFeedInfo{}
	dir := c.MkDir()
	path := filepath.Join(dir, "config.toml")
	content := `
	[filter]
	key-prefix = "key\\x00"
	key-pattern = "key\\x00pattern"
	value-pattern = "value\\ffpattern"
	`
	err = os.WriteFile(path, []byte(content), 0o644)
	c.Assert(err, check.IsNil)
	c.Assert(cmd.ParseFlags([]string{fmt.Sprintf("--config=%s", path)}), check.IsNil)
	newInfo, err = o.applyChanges(oldInfo, cmd)
	c.Assert(err, check.IsNil)
	filterCnf := newInfo.Config.Filter
	c.Assert(filterCnf.KeyPrefix, check.Equals, `key\x00`)
	c.Assert(filterCnf.KeyPattern, check.Equals, `key\x00pattern`)
	c.Assert(filterCnf.ValuePattern, check.Equals, `value\ffpattern`)

	// Test for cli command flags that should be ignored.
	oldInfo = &model.ChangeFeedInfo{SortDir: "."}
	c.Assert(cmd.ParseFlags([]string{"--interact"}), check.IsNil)
	_, err = o.applyChanges(oldInfo, cmd)
	c.Assert(err, check.IsNil)

	oldInfo = &model.ChangeFeedInfo{SortDir: "."}
	c.Assert(cmd.ParseFlags([]string{"--pd=http://127.0.0.1:2379"}), check.IsNil)
	_, err = o.applyChanges(oldInfo, cmd)
	c.Assert(err, check.IsNil)

	// Now tikv-cdc don't support to update startKey & endKey, update startKey & endKey will be ignored
	oldInfo = &model.ChangeFeedInfo{StartKey: "", EndKey: "", Format: "hex"}
	c.Assert(cmd.ParseFlags([]string{"--start-key=abc", "--end-key=edf", "--format=raw"}), check.IsNil)
	newInfo, err = o.applyChanges(oldInfo, cmd)
	c.Assert(err, check.IsNil)
	c.Assert(newInfo.StartKey, check.Equals, "")
	c.Assert(newInfo.EndKey, check.Equals, "")
	c.Assert(newInfo.Format, check.Equals, "hex")

	filename := filepath.Join(dir, "log.txt")
	reset, err := initTestLogger(filename)
	defer reset()
	c.Assert(err, check.IsNil)

	// Test for flag that cannot be updated.
	oldInfo = &model.ChangeFeedInfo{SortDir: "."}
	c.Assert(cmd.ParseFlags([]string{"--sort-dir=/home"}), check.IsNil)
	newInfo, err = o.applyChanges(oldInfo, cmd)
	c.Assert(err, check.IsNil)
	c.Assert(newInfo.SortDir, check.Equals, ".")
	file, err := os.ReadFile(filename)
	c.Assert(err, check.IsNil)
	c.Assert(strings.Contains(string(file), "this flag cannot be updated and will be ignored"), check.IsTrue)
}

func initTestLogger(filename string) (func(), error) {
	logConfig := &log.Config{
		File: log.FileLogConfig{
			Filename: filename,
		},
	}

	logger, props, err := log.InitLogger(logConfig)
	if err != nil {
		return nil, err
	}
	log.ReplaceGlobals(logger, props)

	return func() {
		conf := &log.Config{Level: "info", File: log.FileLogConfig{}}
		logger, props, _ := log.InitLogger(conf)
		log.ReplaceGlobals(logger, props)
	}, nil
}
