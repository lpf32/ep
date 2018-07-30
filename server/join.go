package server

import (
	"os"

	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/juju/errors"
	"github.com/panpan-zhang/ep/pkg/etcdutil"
	log "github.com/sirupsen/logrus"
	"path"
	"strings"
)

func PrepareJoinCluster(cfg *Config) error {
	// - A PD tries to join itself.
	if cfg.Join == "" {
		return nil
	}

	if cfg.Join == cfg.AdvertiseClientUrls {
		return errors.New("join self is forbidden")
	}

	// Cases with data directory.
	initialCluster := ""
	if isDataExist(path.Join(cfg.DataDir, "member")) {
		cfg.InitialCluster = initialCluster
		cfg.InitialClusterState = embed.ClusterStateFlagExisting
		return nil
	}

	// Below are cases without data directory.
	tlsConfig, err := cfg.Security.ToTLSConfig()
	if err != nil {
		return errors.Trace(err)
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(cfg.Join, ","),
		DialTimeout: etcdutil.DefaultDialTimeout,
		TLS:         tlsConfig,
	})
	if err != nil {
		return errors.Trace(err)
	}
	defer client.Close()

	listResp, err := etcdutil.ListEtcdMembers(client)
	if err != nil {
		return errors.Trace(err)
	}

	existed := false
	for _, m := range listResp.Members {
		if m.Name == cfg.Name {
			existed = true
		}
	}

	// - A failed EP re-joins the previous cluster without data directory
	if existed {
		return errors.New("missing data or join a duplicated ep")
	}

	// - A new EP joins an existing cluster.
	// - A deleted EP joins to previous cluster.
	addResp, err := etcdutil.AddEtcdMember(client, []string{cfg.AdvertisePeerUrls})
	if err != nil {
		return errors.Trace(err)
	}

	listResp, err = etcdutil.ListEtcdMembers(client)
	if err != nil {
		return errors.Trace(err)
	}

	pds := []string{}
	for _, memb := range listResp.Members {
		n := memb.Name
		if memb.ID == addResp.Member.ID {
			n = cfg.Name
		}
		for _, m := range memb.PeerURLs {
			pds = append(pds, fmt.Sprintf("%s=%s", n, m))
		}
	}
	initialCluster = strings.Join(pds, ",")
	cfg.InitialCluster = initialCluster
	cfg.InitialClusterState = embed.ClusterStateFlagExisting
	return nil
}

func isDataExist(d string) bool {
	dir, err := os.Open(d)
	if err != nil {
		log.Error("Failed to open:", err)
		return false
	}
	defer dir.Close()

	names, err := dir.Readdirnames(-1)
	if err != nil {
		log.Error("failed to list:", err)
		return false
	}
	return len(names) != 0
}
