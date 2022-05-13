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
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/pkg/tsoutil"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	etcdTimeout = time.Duration(3) * time.Second
	// etcdElectionPath for all gcworker servers.
	etcdElectionPath     = "/gc-worker/election"
	etcdElectionVal      = "local"
	maxPdMsgSize         = int(128 * units.MiB)
	gcWorkerSafePointTTL = math.MaxInt64 // Sets TTL to MAX to make it permanently valid.
	gcWorkerServiceID    = "gc_worker"   // MUST be same with definition in PD
)

// The version info is set in Makefile
var (
	GCWorkerVersion = "None"
)

// LogGCWorkerInfo prints the GC-Worker version information.
func LogGCWorkerInfo() {
	log.Info("Welcome to GC-Worker for TiKV.", zap.String("version", GCWorkerVersion))
}

// PrintGCWorkerInfo prints the GC-Worker version information without log info.
func PrintGCWorkerInfo() {
	fmt.Println("GC-Worker for TiKV Release Version:", GCWorkerVersion)
}

type Server struct {
	// Server state.
	isServing int64
	isLead    int64

	// Server start timestamp
	startTimestamp int64

	// Configs and initial fields.
	cfg *Config

	ctx              context.Context
	serverLoopCtx    context.Context
	serverLoopCancel func()
	serverLoopWg     sync.WaitGroup

	etcdClient *clientv3.Client
	pdClient   pd.Client
}

// CreateServer creates the UNINITIALIZED pd server with given configuration.
func CreateServer(ctx context.Context, cfg *Config) (*Server, error) {
	log.Info("GCWorker Config", zap.Reflect("config", cfg))

	s := &Server{
		cfg:            cfg,
		ctx:            ctx,
		startTimestamp: time.Now().Unix(),
	}

	err := s.createPdClient(ctx)
	if err != nil {
		log.Error("create pd client fail", zap.Error(err), zap.String("worker", s.cfg.Name))
		return nil, err
	}

	err = s.createEtcdClient()
	if err != nil {
		log.Error("create pd client fail", zap.Error(err), zap.String("worker", s.cfg.Name))
		return nil, err
	}
	return s, nil
}

func (s *Server) GetServerName() string {
	return s.cfg.Name
}

func (s *Server) createPdClient(ctx context.Context) error {
	addrs := strings.Split(s.cfg.PdAddrs, ",")
	securityOption := pd.SecurityOption{
		SSLCABytes:   []byte(s.cfg.TLSConfig.CA),
		SSLCertBytes: []byte(s.cfg.TLSConfig.Cert),
		SSLKEYBytes:  []byte(s.cfg.TLSConfig.Key),
	}
	maxCallMsgSize := []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxPdMsgSize)),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(maxPdMsgSize)),
	}
	pdClient, err := pd.NewClientWithContext(
		ctx, addrs, securityOption,
		pd.WithGRPCDialOptions(maxCallMsgSize...),
		pd.WithCustomTimeoutOption(10*time.Second),
		pd.WithMaxErrorRetry(3),
	)
	if err != nil {
		log.Error("fail to create pd client", zap.Error(err), zap.String("worker", s.cfg.Name))
		return errors.Trace(err)
	}
	s.pdClient = pdClient
	return nil
}

func (s *Server) createEtcdClient() error {
	if len(s.cfg.EtcdEndpoint) == 0 {
		return errors.New("No etcd enpoint is specified")
	}
	endpoints := strings.Split(s.cfg.EtcdEndpoint, ",")
	log.Info("create etcd v3 client", zap.Strings("endpoints", endpoints),
		zap.Reflect("cert", s.cfg.TLSConfig), zap.String("worker", s.cfg.Name))

	lgc := zap.NewProductionConfig()
	lgc.Encoding = log.ZapEncodingName
	tlsCfg, err := s.cfg.TLSConfig.ToTLSConfig()
	if err != nil {
		log.Error("tls config is invalid", zap.Error(err), zap.String("worker", s.cfg.Name))
		return err
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: etcdTimeout,
		TLS:         tlsCfg,
		LogConfig:   &lgc,
	})
	if err != nil {
		return errors.Trace(err)
	}
	s.etcdClient = client
	return nil
}

// Close closes the server.
func (s *Server) Close() {
	if s.etcdClient != nil {
		if err := s.etcdClient.Close(); err != nil {
			log.Error("close etcd client meet error", zap.Error(err), zap.String("worker", s.cfg.Name))
		}
	}
	if s.pdClient != nil {
		s.pdClient.Close()
	}

	if !atomic.CompareAndSwapInt64(&s.isServing, 1, 0) {
		// server is already closed
		return
	}

	log.Info("closing server", zap.String("worker", s.cfg.Name))
	s.stopServerLoop()

	log.Info("closed server", zap.String("worker", s.cfg.Name))
}

// checks whether server is closed or not.
func (s *Server) IsServing() bool {
	return atomic.LoadInt64(&s.isServing) != 0
}

func (s *Server) IsLead() bool {
	return atomic.LoadInt64(&s.isLead) != 0
}

func (s *Server) StartServer() {
	atomic.StoreInt64(&s.isServing, 1)
	s.serverLoopCtx, s.serverLoopCancel = context.WithCancel(s.ctx)
	s.serverLoopWg.Add(2)
	go s.startEtcdLoop()
	go s.startUpdateGCSafePointLoop()
}

func (s *Server) stopServerLoop() {
	s.serverLoopCancel()
	s.serverLoopWg.Wait()
}

func (s *Server) startEtcdLoop() {
	defer s.serverLoopWg.Done()
	ctx, cancel := context.WithCancel(s.serverLoopCtx)
	defer cancel()

	for {
		select {
		case <-time.After(s.cfg.EtcdElectionInterval.Duration):
			session, err := concurrency.NewSession(s.etcdClient, concurrency.WithTTL(5))
			if err != nil {
				log.Error("create election session fail", zap.Error(err), zap.String("name", s.cfg.Name))
				continue
			}
			election := concurrency.NewElection(session, etcdElectionPath)
			log.Info("start campaign for leader", zap.String("worker", s.cfg.Name))
			if err = election.Campaign(ctx, etcdElectionVal); err != nil {
				log.Error("campaign fail", zap.Error(err), zap.String("name", s.cfg.Name))
				continue
			}
			atomic.StoreInt64(&s.isLead, 1)
			log.Info("current node become etcd leader", zap.String("worker", s.cfg.Name))
			<-session.Done()
			atomic.StoreInt64(&s.isLead, 0)
			log.Info("etcd election session expired", zap.String("worker", s.cfg.Name))
		case <-ctx.Done():
			atomic.StoreInt64(&s.isLead, 0)
			log.Info("server is closed, exit etcd loop", zap.String("worker", s.cfg.Name))
			return
		}
	}
}

func (s *Server) getGCWorkerSafePoint(ctx context.Context) (uint64, error) {
	physical, logical, err := s.pdClient.GetTS(ctx)
	if err != nil {
		log.Error("fail to get tso", zap.Error(err))
		return 0, errors.Trace(err)
	}
	currentTs := tsoutil.ComposeTS(physical, logical)
	unixTime, _ := tsoutil.ParseTS(currentTs)
	safePointTime := unixTime.Add(-s.cfg.GCLifeTime.Duration)
	gcSafePoint := tsoutil.GenerateTS(tsoutil.GenerateTimestamp(safePointTime, 0))
	return gcSafePoint, nil
}

func (s *Server) calcNewGCSafePoint(serviceSafePoint, gcWorkerSafePoint uint64) uint64 {
	// `serviceSafePoint == 0` means no service is registered. Just use gc safe point.
	if serviceSafePoint > 0 && serviceSafePoint < gcWorkerSafePoint {
		return serviceSafePoint
	}
	return gcWorkerSafePoint
}

/* keep these codes, uncomment these when new pd interfaces are enabled.
func (s *Server) updateServiceGroupSafePointWithRetry(ctx context.Context, serviceGroup string, gcWorkerSafePoint uint64) error {
	succeed := false
	for i := 0; i < updateSafePointRetryCnt; i++ {
		serviceSafePoint, revision, err := s.pdClient.GetMinServiceSafePointByServiceGroup(ctx, serviceGroup)
		if err != nil {
			log.Error("get min service safe point fails.", zap.Error(err),
				zap.String("serviceGroup", serviceGroup), zap.String("worker", s.cfg.Name))
			return errors.Trace(err)
		}
		gcSafePoint := s.calcNewGCSafePoint(serviceSafePoint, gcWorkerSafePoint)
		succeed, newSafePoint, _, err := s.pdClient.UpdateGCSafePointByServiceGroup(ctx, serviceGroup,
			gcSafePoint, revision)
		if !succeed {
			log.Info("update gc safepoint fail", zap.String("serviceGroup", serviceGroup),
				zap.Int("retryCnt", i), zap.String("worker", s.cfg.Name), zap.Error(err))
			continue
		}
		log.Info("update gc safepoint succeed", zap.String("serviceGroup", serviceGroup),
			zap.Uint64("gcWorkerSafePoint", gcWorkerSafePoint),
			zap.Uint64("serviceSafePoint", serviceSafePoint),
			zap.Uint64("newSafePoint", newSafePoint),
			zap.Int("retryCnt", i),
			zap.String("worker", s.cfg.Name))
		break
	}
	if !succeed {
		return errors.Errorf("update %s gc safepoint fail", s.cfg.Name)
	}
	return nil
}

func (s *Server) updateRawGCSafePoint(ctx context.Context) error {
	allServiceGroups, err := s.pdClient.GetServiceGroup(ctx)
	if err != nil {
		log.Error("get service group from gc failed", zap.Error(err), zap.String("worker", s.cfg.Name))
		return errors.Trace(err)
	}

	gcWorkerSafePoint, err := s.getGCWorkerSafePoint(ctx)
	if err != nil {
		log.Error("calc gc-worker safe point fails.", zap.Error(err), zap.String("worker", s.cfg.Name))
		return errors.Trace(err)
	}

	// TODO: Different service group may need different update frequency.
	for _, serviceGroup := range allServiceGroups {
		err = s.updateServiceGroupSafePointWithRetry(ctx, serviceGroup, gcWorkerSafePoint)
		if err != nil {
			log.Error("update gc safepoint fail, will retry next time.",
				zap.String("serviceGroup", serviceGroup), zap.String("worker", s.cfg.Name))
		}
	}
	return nil
}
*/

func (s *Server) updateRawGCSafePoint(ctx context.Context) error {
	gcWorkerSafePoint, err := s.getGCWorkerSafePoint(ctx)
	if err != nil {
		log.Error("calc gc-worker safe point fails.", zap.Error(err), zap.String("worker", s.cfg.Name))
		return errors.Trace(err)
	}
	serviceSafePoint, err := s.pdClient.UpdateServiceGCSafePoint(ctx, gcWorkerServiceId, gcWorkerSafePointTtl, gcWorkerSafePoint)
	if err != nil {
		log.Error("update service gc safepoint fails", zap.Error(err), zap.String("worker", s.cfg.Name))
		return errors.Trace(err)
	}
	newSafepoint := s.calcNewGCSafePoint(serviceSafePoint, gcWorkerSafePoint)
	retSafePoint, err := s.pdClient.UpdateGCSafePoint(ctx, newSafepoint)
	if err != nil {
		log.Error("update gc safepoint fails", zap.Error(err), zap.String("worker", s.cfg.Name))
		return errors.Trace(err)
	}
	log.Info("update gc safepoint with old finish", zap.Uint64("gcWorkerSafePoint", gcWorkerSafePoint),
		zap.Uint64("serviceSafePoint", serviceSafePoint),
		zap.Uint64("newSafepoint", newSafepoint),
		zap.Uint64("retSafePoint", retSafePoint),
		zap.Uint64("gcWorkerSafePoint", gcWorkerSafePoint),
		zap.String("worker", s.cfg.Name))
	return nil
}

func (s *Server) startUpdateGCSafePointLoop() {
	defer s.serverLoopWg.Done()
	ctx, cancel := context.WithCancel(s.serverLoopCtx)
	defer cancel()

	for {
		select {
		case <-time.After(s.cfg.SafePointUpdateInterval.Duration):
			if !s.IsServing() || !s.IsLead() {
				log.Info("current node is not serving or not lead", zap.String("worker", s.cfg.Name),
					zap.Bool("serving", s.IsServing()), zap.Bool("isLead", s.IsLead()))
				continue
			}
			if err := s.updateRawGCSafePoint(ctx); err != nil {
				continue
			}

		case <-ctx.Done():
			log.Info("server is closed, exit update gcSafePoint loop", zap.String("worker", s.cfg.Name))
			return
		}
	}
}
