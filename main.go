package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"time"

	"github.com/deatheyes/mongo-proxy/config"
	"github.com/deatheyes/mongo-proxy/meta"
	"github.com/deatheyes/mongo-proxy/proxy"
	"github.com/golang/glog"
)

var (
	version bool
	conf    string
	port    string
	logDir  string
	psm     string

	// version info
	sha1   string
	date   string
	branch string
)

func init() {
	flag.StringVar(&conf, "conf_dir", "./conf", "config file directory")
	flag.StringVar(&port, "port", "27019", "proxy port")
	flag.BoolVar(&version, "version", false, "show version")
	glog.MaxSize = 1 << 28
}

func showVersion() {
	fmt.Printf("mongo-proxy - author yanyu - https://github.com/deatheyes/mongo-proxy - %s - %s - %s", branch, date, sha1)
}

func main() {
	flag.Parse()
	if version {
		showVersion()
		return
	}

	address := ":" + port
	if len(port) == 0 {
		address = ":27019"
	}

	fmt.Println("serve on ", port)

	c, err := config.FromFile(filepath.Join(conf, "config.yaml"))
	if err != nil {
		glog.Fatalf("get config from failed: %v", err)
		return
	}

	glog.Infof("mongo-proxy - branch: %s, date: %s, sha1: %s", branch, date, sha1)

	// create meta client
	clusterMetaClient, err := meta.CreateClient(c.Meta.Cluster)
	if err != nil {
		glog.Fatalf("init cluster meta client failed: %v", err)
		return
	}
	if err := clusterMetaClient.Connect(context.Background()); err != nil {
		glog.Fatalf("connect cluster meta server faild: %v", err)
		return
	}
	authMetaClient, err := meta.CreateClient(c.Meta.Auth)
	if err != nil {
		glog.Fatalf("init auth meta client failed: %v", err)
		return
	}
	if err := authMetaClient.Connect(context.Background()); err != nil {
		glog.Fatalf("connect auth meta server faild: %v", err)
		return
	}

	// create and run proxy
	if err := proxy.New().
		ClusterMetaServer(clusterMetaClient).
		AuthMetaServer(authMetaClient).
		AliveTimeout(time.Duration(c.Connection.AliveTimeoutSecond) * time.Second).
		Serve(address); err != nil {
		glog.Fatalf("init proxy failed: %v", err)
		return
	}

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	<-interrupt
}
