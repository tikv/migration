// Copyright 2022 TiKV Project Authors.
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

package server

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestServerConfig(t *testing.T) {

	pd := "127.0.0.0.1:1234"
	name := "gc-worker"
	logLevel := "debug"
	logFile := "/tmp/gc-worker.log"
	configFile := "/tmp/config.toml"
	cfgStr := "--pd " + pd + " --name " + name + " --L " + logLevel +
		" --log-file " + logFile + " --config " + configFile
	cfg := NewConfig()
	cfgStrArr := strings.Split(cfgStr, " ")
	cfg.Parse(cfgStrArr)
	require.Equal(t, pd, cfg.PdAddrs)
	require.Equal(t, name, cfg.Name)
	require.Equal(t, logFile, cfg.Log.File.Filename)
	require.Equal(t, logLevel, cfg.Log.Level)
	require.Equal(t, configFile, cfg.GetConfigFile())
	require.Equal(t, false, cfg.Version)

	cfgStr = "--version"
	cfgStrArr = strings.Split(cfgStr, " ")
	cfg.Parse(cfgStrArr)
	require.Equal(t, true, cfg.Version)

}
