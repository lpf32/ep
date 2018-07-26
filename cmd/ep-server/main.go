package main

import (
	"context"
	"flag"
	"github.com/juju/errors"
	"github.com/panpan-zhang/ep/server"
	log "github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	cfg := server.NewConfig()
	err := cfg.Parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		log.Fatalf("parse cmd flags error: %s\n", errors.ErrorStack(err))
	}

	err = server.PrepareJoinCluster(cfg)
	if err != nil {
		log.Fatal("join error ", errors.ErrorStack(err))
	}
	svr, err := server.CreateServer(cfg)
	if err != nil {
		log.Fatalf("create server failed: %v", errors.ErrorStack(err))
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	ctx, cancel := context.WithCancel(context.Background())
	var sig os.Signal
	go func() {
		sig = <-sc
		cancel()
	}()

	if err := svr.Run(ctx); err != nil {
		log.Fatalf("run server failed: %v", errors.ErrorStack(err))
	}

	<-ctx.Done()
	log.Infof("Got signal [%d] to exit.", sig)

	switch sig {
	case syscall.SIGTERM:
		os.Exit(0)
	default:
		os.Exit(1)
	}
}
