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
	"github.com/davecgh/go-spew/spew"
	"github.com/google/uuid"
	clientv3 "go.etcd.io/etcd/client/v3"
)

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

func (b *Provider) Put(key, value string) error {
	if _, err := b.KV.Put(context.Background(), key, value); err != nil {
		return err
	}
	return nil
}

func tf(t time.Time) string {
	return t.Format(time.RFC3339)
}

func (b *Provider) GetCurrentTasks() error {
	now := time.Now()
	log.Printf("Getting scheduled messages older than %s", tf(now))
	items := map[uuid.UUID]*schedule.Task{}
	op := clientv3.OpGet("schedule/0", clientv3.WithFromKey(), clientv3.WithLimit(2), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	var outOfRange bool

	for !outOfRange {
		res, err := b.Do(context.Background(), op)
		if err != nil {
			return err
		}

		if len(res.Get().Kvs) == 0 {
			break
		}

		for _, item := range res.Get().Kvs {
			ts, err := parseTimestampFromKey(item.Key)
			if err != nil {
				return err
			}

			log.Println(tf(ts))
			log.Printf("%s is before %s: %t", tf(ts), tf(now), ts.Before(now))
			if !ts.Before(now) {
				outOfRange = true
				break
			}

			valBuf := bytes.NewBuffer(item.Value)
			decoder := json.NewDecoder(valBuf)

			var task schedule.Task
			if err := decoder.Decode(&task); err != nil {
				return err
			}

			items[task.ID] = &task
			op = clientv3.OpGet(fmt.Sprintf("schedule/%d", ts.UnixNano()+1), clientv3.WithFromKey(), clientv3.WithLimit(2), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
		}
		// outOfRange = true
	}

	// spew.Dump(res)
	spew.Dump(items)
	return nil
	// b.KV.Get(context.Background())
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
