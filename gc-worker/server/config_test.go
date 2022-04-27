// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package server

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestServerConfig(t *testing.T) {

	pd := "127.0.0.0.1:1234"
	name := "gc-worker"
	etcd := "127.0.0.1:2345"
	logLevel := "debug"
	logFile := "/tmp/gc-worker.log"
	configFile := "/tmp/config.toml"
	cfgStr := "--pd " + pd + " --name " + name + " --etcd " + etcd + " --L " + logLevel +
		" --log-file " + logFile + " --config " + configFile
	cfg := NewConfig()
	cfgStrArr := strings.Split(cfgStr, " ")
	cfg.Parse(cfgStrArr)
	require.Equal(t, pd, cfg.PdAddrs)
	require.Equal(t, name, cfg.Name)
	require.Equal(t, etcd, cfg.EtcdEndpoint)
	require.Equal(t, logFile, cfg.Log.File.Filename)
	require.Equal(t, logLevel, cfg.Log.Level)
	require.Equal(t, configFile, cfg.GetConfigFile())
	require.Equal(t, false, cfg.Version)

	cfgStr = "--version"
	cfgStrArr = strings.Split(cfgStr, " ")
	cfg.Parse(cfgStrArr)
	require.Equal(t, true, cfg.Version)

}
