package main

import (
	"flag"
	"net"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/0xa1-red/an-dagda/api"
	intetcd "github.com/0xa1-red/an-dagda/backend/etcd"
	"github.com/0xa1-red/an-dagda/task"
	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/cluster"
	"github.com/asynkron/protoactor-go/cluster/clusterproviders/etcd"
	"github.com/asynkron/protoactor-go/cluster/identitylookup/partition"
	logmod "github.com/asynkron/protoactor-go/log"
	"github.com/asynkron/protoactor-go/remote"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	endpoints string
	plog      = logmod.New(logmod.InfoLevel, "[DAEMON][main]")
)

func main() {
	flag.StringVar(&endpoints, "endpoints", "127.0.0.1:2379", "Comma-separated list of etcd endpoints")
	flag.Parse()

	endpoints := strings.Split(endpoints, ",")
	if len(endpoints) == 0 {
		plog.Error("No endpoints specified")
		os.Exit(1)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt)

	backendProvider, err := intetcd.New(endpoints)
	if err != nil {
		panic(err)
	}
	plog.Info("Backend provider created")

	system := actor.NewActorSystem()

	cluster.SetLogLevel(logmod.InfoLevel)
	remote.SetLogLevel(logmod.InfoLevel)
	partition.SetLogLevel(logmod.InfoLevel)

	protoProvider, err := etcd.NewWithConfig("/protoactor", clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: time.Second * 5,
	})
	if err != nil {
		panic(err)
	}
	plog.Info("Protoactor provider created")

	lookup := partition.New()
	ip := getOutboundIP()
	plog.Info("Advertising host " + ip.String() + ":56601")

	config := remote.Configure("0.0.0.0", 56601, remote.WithAdvertisedHost(ip.String()+":56601"))

	schedulerKind := task.NewSchedulerKind(func() task.Scheduler {
		return task.NewScheduler(backendProvider)
	}, 0)

	processorKind := task.NewTaskProcessorKind(func() task.TaskProcessor {
		return &task.TaskProcessorGrain{}
	}, 0)

	clusterConfig := cluster.Configure("an-dagda", protoProvider, lookup, config,
		cluster.WithKinds(schedulerKind), cluster.WithKinds(processorKind))

	c := cluster.New(system, clusterConfig)
	c.StartMember()
	plog.Info("Cluster member started")
	defer c.Shutdown(true)

	client := task.GetSchedulerGrainClient(c, task.OverseerID.String())
	if _, err := client.Start(&task.Empty{}); err != nil {
		plog.Error("Couldn't start overseer", logmod.Error(err))
		os.Exit(1)
	}

	apiServer := api.New(c)

	<-sigs
	apiServer.Stop()
	client.Stop(&task.Empty{}) // nolint
	c.Shutdown(true)
}

func getOutboundIP() net.IP { //nolint
	// this tries to connect to a fake UDP service, we just need the conn to be created
	conn, err := net.Dial("udp", "github.com:80")
	if err != nil {
		plog.Error("Failed to infer host IP", logmod.Error(err))
		os.Exit(1)
	}
	defer conn.Close()

	return conn.LocalAddr().(*net.UDPAddr).IP
}
