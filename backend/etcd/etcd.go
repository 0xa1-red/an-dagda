package etcd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/0xa1-red/an-dagda/schedule"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Provider is an etcd backend
// TODO: Add a way to lock items currently being queried so they aren't accessed from anywhere else
type Provider struct {
	*clientv3.Client
}

func New(endpoints []string) (*Provider, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	backend := Provider{
		Client: cli,
	}
	return &backend, nil
}

func (b *Provider) Put(ctx context.Context, key, value string) error {
	if _, err := b.KV.Put(ctx, key, value); err != nil {
		return err
	}
	return nil
}

func (b *Provider) Delete(ctx context.Context, key, value string) error {
	if _, err := b.KV.Delete(ctx, key); err != nil {
		return err
	}
	return nil
}

func tf(t time.Time) string {
	return t.Format(time.RFC3339)
}

func (b *Provider) GetCurrentTasks() ([]*schedule.Task, error) {
	now := time.Now()
	log.Printf("Getting scheduled messages older than %s", tf(now))
	items := []*schedule.Task{}
	op := clientv3.OpGet("schedule/0", clientv3.WithFromKey(), clientv3.WithRange(fmt.Sprintf("schedule/%d", now.UnixNano())), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))

	res, err := b.Do(context.Background(), op)
	if err != nil {
		return nil, err
	}

	for _, item := range res.Get().Kvs {
		valBuf := bytes.NewBuffer(item.Value)
		decoder := json.NewDecoder(valBuf)

		var task schedule.Task
		if err := decoder.Decode(&task); err != nil {
			return nil, err
		}

		items = append(items, &task)
	}

	return items, nil
}

func (b *Provider) Close() error {
	return b.Client.Close()
}

func parseTimestampFromKey(key []byte) (time.Time, error) {
	split := strings.Split(string(key), "/")
	tsUnixNano, err := strconv.ParseInt(split[1], 10, 64)
	if err != nil {
		return time.Time{}, err
	}

	return time.Unix(0, tsUnixNano), nil
}
