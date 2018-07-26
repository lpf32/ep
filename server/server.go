package server

import (
	"math/rand"
	"time"

	"context"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/juju/errors"
	"github.com/panpan-zhang/ep/pkg/etcdutil"
	log "github.com/sirupsen/logrus"
	"strings"
)

const (
	etcdTimeout = time.Second * 3
	// pdRootPath for all pd servers.
	pdRootPath      = "/ep"
	pdAPIPrefix     = "/ep/"
	pdClusterIDPath = "/ep/cluster_id"
)

type Server struct {
	// Server state.
	isServer int64
	isLeader int64

	// Configs and initial fields
	cfg     *Config
	etcdCfg *embed.Config

	// Etcd and cluster informations.
	ectd   *embed.Etcd
	client *clientv3.Client
	id     uint64 // etcd server id.
}

// CreateServer creates the UNINITIALIZED ep server with given configuration.
func CreateServer(cfg *Config) (*Server, error) {
	log.Infof("EP config - %v", cfg)
	rand.Seed(time.Now().UnixNano())

	s := &Server{
		cfg: cfg,
	}

	// Adjust etcd config.
	etcdCfg, err := s.cfg.genEmbedEtcdConfig()
	if err != nil {
		return nil, errors.Trace(err)
	}
	s.etcdCfg = etcdCfg

	return s, nil
}

func (s *Server) startEtcd(ctx context.Context) error {
	log.Info("start embed etcd")
	etcd, err := embed.StartEtcd(s.etcdCfg)
	if err != nil {
		return errors.Trace(err)
	}

	select {
	// Wait etcd until it is ready to use
	case <-etcd.Server.ReadyNotify():
	case <-ctx.Done():
		return errors.Errorf("canceled when waiting embed etcd to be ready")
	}

	endpoints := []string{s.etcdCfg.ACUrls[0].String()}
	log.Infof("create etcd v3 client with endpoints %v", endpoints)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: etcdTimeout,
	})
	if err != nil {
		return errors.Trace(err)
	}

	etcdServerID := uint64(etcd.Server.ID())

	// update advertise peer urls.
	etcdMembers, err := etcdutil.ListEtcdMembers(client)
	if err != nil {
		return errors.Trace(err)
	}
	for _, m := range etcdMembers.Members {
		if etcdServerID == m.ID {
			etcdPeerURLs := strings.Join(m.PeerURLs, ",")
			if s.cfg.AdvertisePeerUrls != etcdPeerURLs {
				log.Infof("update advertise peer urls from %s to %s", s.cfg.AdvertisePeerUrls, etcdPeerURLs)
			}
		}
	}

	s.ectd = etcd
	s.client = client
	s.id = etcdServerID
	return nil
}

func (s *Server) Run(ctx context.Context) error {
	if err := s.startEtcd(ctx); err != nil {
		return errors.Trace(err)
	}

	return nil
}
