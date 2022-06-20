package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/0xa1-red/an-dagda/backend/etcd"
	"github.com/0xa1-red/an-dagda/schedule"
	"github.com/asynkron/protoactor-go/log"
)

var (
	endpoints = []string{"127.0.0.1:2379"}
	plog      = log.New(log.InfoLevel, "[CLIENT][main]")
)

func main() {
	etcdProvider, err := etcd.New(endpoints)
	if err != nil {
		panic(err)
	}
	defer etcdProvider.Close()

	today := time.Now().Format("2006-01-02")

	tasks := []struct {
		message   string
		timestamp string
	}{
		{
			message:   "asdasdasd",
			timestamp: fmt.Sprintf("%s 13:00:00", today),
		},
		{
			message:   "asdasdasd #2",
			timestamp: fmt.Sprintf("%s 12:00:00", today),
		},
		// {
		// 	message:   "First message",
		// 	timestamp: fmt.Sprintf("%s 14:00:00", today),
		// },
		// {
		// 	message:   "Second message",
		// 	timestamp: fmt.Sprintf("%s 15:00:00", today),
		// },
		// {
		// 	message:   "Third message",
		// 	timestamp: fmt.Sprintf("%s 16:00:00", today),
		// },
		// {
		// 	message:   "Fourth message",
		// 	timestamp: fmt.Sprintf("%s 17:00:00", today),
		// },
		// {
		// 	message:   "Fifth message",
		// 	timestamp: fmt.Sprintf("%s 18:00:00", today),
		// },
	}

	for _, task := range tasks {
		t := schedule.NewTask("test", task.message)
		ts, err := time.Parse("2006-01-02 15:04:05", task.timestamp)
		if err != nil {
			plog.Error("Failed to parse timestamp", log.Error(err))
			os.Exit(1)
		}
		if err := etcdProvider.Schedule(context.TODO(), t, ts); err != nil {
			plog.Error("Failed to schedule task", log.Error(err), log.String("message", t.Data))
			os.Exit(1)
		}
	}
}
