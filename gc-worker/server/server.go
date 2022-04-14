package server

import (
	"context"
	"flag"
	"fmt"
	"gc-worker/server/config"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	pd "github.com/tikv/pd/client"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	etcdTimeout = time.Second * 3
	// gcWorkerRootPath for all gcworker servers.
	gcWorkerRootPath      = "/gc-worker/selection"
	gcWorkerElectionVal   = "local"
	maxMsgSize            = int(128 * units.MiB)
	updateGCPointInterval = 10 * 1000 * 1000 * 1000 // 10 minutes
	etcdCampaignInterval  = 10 * 1000 * 1000        // 1 ms

	physicalShiftBits = 18
	logicalBits       = (1 << physicalShiftBits) - 1
)

var (
	// EtcdStartTimeout the timeout of the startup etcd.
	EtcdStartTimeout = time.Minute * 5
	gcLifetime       = flag.Duration("lifetime", time.Minute*10, "Time during which the data should be kept. default: 10m")
)

// LogPDInfo prints the PD version information.
func LogGCWorkerInfo() {
	log.Info("Welcome to GC-Worker for TiKV")
}

// PrintPDInfo prints the PD version information without log info.
func PrintGCWorkerInfo() {
	fmt.Printf("GC Worker for TiKV version 0.1.0")
}

type Server struct {
	// Server state.
	isServing int64
	isLead    bool

	// Server start timestamp
	startTimestamp int64

	// Configs and initial fields.
	cfg *config.Config

	ctx              context.Context
	serverLoopCtx    context.Context
	serverLoopCancel func()
	serverLoopWg     sync.WaitGroup
	// etcd client
	etcdClient *clientv3.Client
	pdClient   pd.Client
}

// CreateServer creates the UNINITIALIZED pd server with given configuration.
func CreateServer(ctx context.Context, cfg *config.Config) (*Server, error) {
	log.Info("GCWorker Config", zap.Reflect("config", cfg))

	s := &Server{
		cfg:            cfg,
		ctx:            ctx,
		startTimestamp: time.Now().Unix(),
	}

	err := s.createPdClient(ctx)
	if err != nil {
		log.Error("create pd client fail", zap.Error(err))
		return nil, err
	}

	err = s.createEtcdClient(ctx)
	if err != nil {
		log.Error("create pd client fail", zap.Error(err))
		return nil, err
	}
	return s, nil
}

func (s *Server) GetServerName() string {
	return s.cfg.Name
}

func (s *Server) createPdClient(ctx context.Context) error {
	addrs := strings.Split(s.cfg.PdAddrs, ",")
	securityOption := pd.SecurityOption{}
	maxCallMsgSize := []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSize)),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(maxMsgSize)),
	}
	pdClient, err := pd.NewClientWithContext(
		ctx, addrs, securityOption,
		pd.WithGRPCDialOptions(maxCallMsgSize...),
		pd.WithCustomTimeoutOption(10*time.Second),
		pd.WithMaxErrorRetry(3),
	)
	if err != nil {
		log.Error("fail to create pd client", zap.Error(err))
		return errors.Trace(err)
	}
	s.pdClient = pdClient
	return nil
}

func (s *Server) createEtcdClient(ctx context.Context) error {
	endpoints := strings.Split(s.cfg.EtcdEndpoint, ",")
	log.Info("create etcd v3 client", zap.Strings("endpoints", endpoints), zap.Reflect("cert", s.cfg.Security))

	lgc := zap.NewProductionConfig()
	lgc.Encoding = log.ZapEncodingName
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: etcdTimeout,
		TLS:         nil,
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
	if !atomic.CompareAndSwapInt64(&s.isServing, 1, 0) {
		// server is already closed
		return
	}

	log.Info("closing server")
	s.stopServerLoop()

	if s.etcdClient != nil {
		if err := s.etcdClient.Close(); err != nil {
			log.Error("close etcd client meet error", zap.Error(err))
		}
	}

	if s.pdClient != nil {
		s.pdClient.Close()
	}

	log.Info("closed server")
}

// checks whether server is closed or not.
func (s *Server) IsServing() bool {
	return atomic.LoadInt64(&s.isServing) != 0
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

// ComposeTS creates a ts from physical and logical parts.
func ComposeTS(physical, logical int64) uint64 {
	return uint64((physical << physicalShiftBits) + logical)
}

// ExtractPhysical returns a ts's physical part.
func ExtractPhysical(ts uint64) int64 {
	return int64(ts >> physicalShiftBits)
}

// ExtractLogical return a ts's logical part.
func ExtractLogical(ts uint64) int64 {
	return int64(ts & logicalBits)
}

// GetPhysical returns physical from an instant time with millisecond precision.
func GetPhysical(t time.Time) int64 {
	return t.UnixNano() / int64(time.Millisecond)
}

// GetTimeFromTS extracts time.Time from a timestamp.
func GetTimeFromTS(ts uint64) time.Time {
	ms := ExtractPhysical(ts)
	return time.Unix(ms/1e3, (ms%1e3)*1e6)
}

func (s *Server) startEtcdLoop() {
	defer s.serverLoopWg.Done()
	ctx, cancel := context.WithCancel(s.serverLoopCtx)
	defer cancel()

	for {
		select {
		case <-time.After(etcdCampaignInterval):
			session, err := concurrency.NewSession(s.etcdClient, concurrency.WithTTL(5))
			if err != nil {
				log.Error("create election session fail", zap.Error(err), zap.String("name", s.cfg.Name))
				continue
			}
			election := concurrency.NewElection(session, gcWorkerRootPath)
			log.Info("start campaign for leader", zap.String("worker", s.cfg.Name))
			if err = election.Campaign(ctx, gcWorkerElectionVal); err != nil {
				log.Error("campaign fail", zap.Error(err), zap.String("name", s.cfg.Name))
				continue
			}
			s.isLead = true
			log.Info("current node become etcd leader", zap.String("worker", s.cfg.Name))
		case <-ctx.Done():
			s.isLead = false
			log.Info("server is closed, exit metrics loop")
			return
		}
	}
}

func (s *Server) startUpdateGCSafePointLoop() {
	defer s.serverLoopWg.Done()
	ctx, cancel := context.WithCancel(s.serverLoopCtx)
	defer cancel()

	for {
		select {
		case <-time.After(updateGCPointInterval):
			if !s.IsServing() || !s.isLead {
				log.Info("current node is not serving or not lead.",
					zap.Bool("serving", s.IsServing()), zap.Bool("isLead", s.isLead))
				continue
			}
			physical, logical, err := s.pdClient.GetTS(ctx)
			if err != nil {
				log.Error("fail to get tso", zap.Error(err))
				continue
			}
			currentTs := ComposeTS(physical, logical)
			currentTime := GetTimeFromTS(currentTs)
			safePointTime := currentTime.Add(-*gcLifetime)
			gcworkerSafePoint := ComposeTS(GetPhysical(safePointTime), 0)
			ttl := int64(time.Hour / time.Second)
			serviceSafePoint, err := s.pdClient.UpdateServiceGCSafePoint(ctx, "gc", ttl, gcworkerSafePoint)
			if err != nil {
				log.Error("update service gc safepoint fails", zap.Error(err))
				continue
			}
			safePoint, err := s.pdClient.UpdateGCSafePoint(ctx, serviceSafePoint)
			if err != nil {
				log.Error("update gc safepoint fails", zap.Error(err))
				continue
			}
			log.Info("update gc safepoint", zap.Int64("tso physical", physical),
				zap.Int64("tso logical", logical),
				zap.Uint64("gcworker sp", gcworkerSafePoint),
				zap.Uint64("service sp", serviceSafePoint),
				zap.Uint64("gc sp", safePoint))
		case <-ctx.Done():
			log.Info("server is closed, exit metrics loop")
			return
		}
	}
}
