package main

import (
	"log"
	"os"
	"os/signal"

	"github.com/0xa1-red/an-dagda/task"
	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/cluster"
	"github.com/asynkron/protoactor-go/cluster/clusterproviders/etcd"
	"github.com/asynkron/protoactor-go/cluster/identitylookup/partition"
	plog "github.com/asynkron/protoactor-go/log"
	"github.com/asynkron/protoactor-go/remote"
)

func main() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt)

	system := actor.NewActorSystem()

	cluster.SetLogLevel(plog.InfoLevel)
	remote.SetLogLevel(plog.InfoLevel)
	partition.SetLogLevel(plog.InfoLevel)

	provider, err := etcd.New()
	if err != nil {
		panic(err)
	}
	lookup := partition.New()
	config := remote.Configure("127.0.0.1", 0)

	schedulerKind := task.NewSchedulerKind(func() task.Scheduler {
		return &task.SchedulerGrain{}
	}, 0)

	processorKind := task.NewTaskProcessorKind(func() task.TaskProcessor {
		return &task.TaskProcessorGrain{}
	}, 0)

	clusterConfig := cluster.Configure("an-dagda", provider, lookup, config,
		cluster.WithKinds(schedulerKind), cluster.WithKinds(processorKind))

	c := cluster.New(system, clusterConfig)
	c.StartMember()
	defer c.Shutdown(true)

	client := task.GetSchedulerGrainClient(c, task.OverseerID.String())
	if _, err := client.Start(&task.Empty{}); err != nil {
		log.Printf("Couldn't start overseer: %v", err)
		os.Exit(1)
	}

	<-sigs
	client.Stop(&task.Empty{}) // nolint
	c.Shutdown(true)
}
