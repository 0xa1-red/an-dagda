package etcd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/0xa1-red/an-dagda/schedule"
	"github.com/asynkron/protoactor-go/log"
	logmod "github.com/asynkron/protoactor-go/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

var (
	plog = logmod.New(logmod.InfoLevel, "[GRAIN][task]")
)

type taskMutex struct {
	key   string
	ttl   time.Time
	mutex *concurrency.Mutex
}

// Provider is an etcd backend
// TODO: Add a way to lock items currently being queried so they aren't accessed from anywhere else
type Provider struct {
	*clientv3.Client

	session *concurrency.Session
	mutexes *sync.Map
}

func New(endpoints []string) (*Provider, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	session, err := concurrency.NewSession(cli)
	if err != nil {
		cli.Close() // nolint
		return nil, err
	}

	backend := Provider{
		Client: cli,

		session: session,
		mutexes: &sync.Map{},
	}
	return &backend, nil
}

func (b *Provider) Put(ctx context.Context, key, value string) error {
	if _, err := b.KV.Put(ctx, key, value); err != nil {
		return err
	}
	return nil
}

func (b *Provider) Delete(ctx context.Context, key string) error {
	if _, err := b.KV.Delete(ctx, key); err != nil {
		return err
	}
	return nil
}

func (b *Provider) GetCurrentTasks(ctx context.Context) ([]*schedule.Task, error) {
	now := time.Now()
	rangeTo := fmt.Sprintf("schedule/%d", now.UnixNano())
	plog.Debug("Getting scheduled messages", log.String("range-end", rangeTo))
	items := []*schedule.Task{}
	op := clientv3.OpGet("schedule/0", clientv3.WithFromKey(), clientv3.WithRange(rangeTo), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))

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

		if err := b.LockTask(context.Background(), &task, 1*time.Hour); err != nil {
			plog.Error("Error locking task", log.String("task-id", task.ID.String()))
			continue
		}
		items = append(items, &task)
	}

	return items, nil
}

func (b *Provider) Schedule(ctx context.Context, t *schedule.Task, s time.Time) error {
	t.ScheduledAt = s
	buf := bytes.NewBuffer([]byte(""))
	encoder := json.NewEncoder(buf)
	if err := encoder.Encode(t); err != nil {
		return err
	}

	key := t.Key()
	value := buf.String()

	return b.Put(context.Background(), key, value)
}

func (b *Provider) Close() error {
	return b.Client.Close()
}

func (b *Provider) LockTask(ctx context.Context, task *schedule.Task, ttl time.Duration) error {
	key := task.Key()
	etcdMx := concurrency.NewMutex(b.session, key)
	if err := etcdMx.Lock(ctx); err != nil {
		return err
	}
	mx := taskMutex{
		key:   key,
		ttl:   time.Now().Add(ttl),
		mutex: etcdMx,
	}

	b.mutexes.Store(key, &mx)
	return nil
}

func (b *Provider) UnlockTask(ctx context.Context, task *schedule.Task) error {
	key := task.Key()
	if mutex, ok := b.mutexes.Load(key); ok {
		mx := mutex.(*taskMutex)
		if err := mx.mutex.Unlock(ctx); err != nil {
			return err
		}
		b.mutexes.Delete(key)
		return nil
	}
	return fmt.Errorf("Mutex %s not found", key)
}
