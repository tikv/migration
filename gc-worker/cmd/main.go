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

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/pingcap/log"
	"github.com/tikv/migration/gc-worker/server"
	"go.uber.org/zap"
)

func main() {
	cfg := server.NewConfig()
	err := cfg.Parse(os.Args[1:])
	if err != nil {
		fmt.Printf("parse cmd flags error, %v", err)
		exit(1)
	}

	if cfg.Version {
		server.PrintGCWorkerInfo()
		exit(0)
	}

	// New zap logger
	err = cfg.SetupLogger()
	if err == nil {
		log.ReplaceGlobals(cfg.GetZapLogger(), cfg.GetZapLogProperties())
	} else {
		fmt.Printf("initialize logger error, %v", err)
		exit(1)
	}
	// Flushing any buffered log entries
	defer log.Sync()

	server.LogGCWorkerInfo()

	// Creates server.
	ctx, cancel := context.WithCancel(context.Background())
	svr, err := server.CreateServer(ctx, cfg)
	if err != nil {
		fmt.Printf("fail to start GCWorker, %v", err)
		exit(1)
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	var sig os.Signal
	go func() {
		sig = <-sc
		cancel()
	}()

	svr.StartServer()

	log.Info("start server end", zap.String("worker", svr.GetServerName()))

	<-ctx.Done()
	log.Info("Got signal to exit", zap.String("signal", sig.String()))

	svr.Close()
	switch sig {
	case syscall.SIGTERM:
		exit(0)
	default:
		exit(1)
	}
}

func exit(code int) {
	log.Sync()
	os.Exit(code)
}
