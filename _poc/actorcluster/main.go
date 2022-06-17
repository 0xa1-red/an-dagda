package main

import (
	"os"
	"os/signal"

	ouractor "github.com/0xa1-red/an-dagda/actor"
	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/cluster"
	"github.com/asynkron/protoactor-go/cluster/clusterproviders/etcd"
	"github.com/asynkron/protoactor-go/cluster/identitylookup/partition"
	"github.com/asynkron/protoactor-go/remote"
)

func main() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt)

	system := actor.NewActorSystem()

	provider, err := etcd.New()
	if err != nil {
		panic(err)
	}
	lookup := partition.New()
	config := remote.Configure("127.0.0.1", 0)

	schedulerKind := ouractor.NewSchedulerKind(func() ouractor.Scheduler {
		return &ouractor.SchedulerGrain{}
	}, 0)

	clusterConfig := cluster.Configure("an-dagda", provider, lookup, config,
		cluster.WithKinds(schedulerKind))

	c := cluster.New(system, clusterConfig)
	c.StartMember()

	<-sigs
	c.Shutdown(true)
}
