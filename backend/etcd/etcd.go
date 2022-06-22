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
	"go.opentelemetry.io/otel/attribute"
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
	_, span := tracer.Start(ctx, "backend-put")
	if id := ctx.Value("task-id"); id != nil {
		taskID := id.(string)
		span.SetAttributes(attribute.String("key", taskID))
	}
	defer span.End()
	if _, err := b.KV.Put(ctx, key, value); err != nil {
		return err
	}
	return nil
}

func (b *Provider) Delete(ctx context.Context, key string) error {
	_, span := tracer.Start(ctx, "backend-delete")
	if id := ctx.Value("task-id"); id != nil {
		taskID := id.(string)
		span.SetAttributes(attribute.String("key", taskID))
	}
	defer span.End()
	if _, err := b.KV.Delete(ctx, key); err != nil {
		return err
	}
	return nil
}

func (b *Provider) GetCurrentTasks(ctx context.Context) ([]*schedule.Task, error) {
	sctx, span := tracer.Start(ctx, "get-tasks")
	defer span.End()
	now := time.Now()
	rangeTo := fmt.Sprintf("schedule/%d", now.UnixNano())
	plog.Debug("Getting scheduled messages", log.String("range-end", rangeTo))
	items := []*schedule.Task{}
	op := clientv3.OpGet("schedule/0", clientv3.WithFromKey(), clientv3.WithRange(rangeTo), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))

	res, err := b.Do(sctx, op)
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
	span.SetAttributes(attribute.Int("len", len(items)))
	return items, nil
}

func (b *Provider) Schedule(ctx context.Context, t *schedule.Task, s time.Time) error {
	sctx, span := tracer.Start(ctx, "schedule")
	sctx = context.WithValue(sctx, "task-id", t.ID.String())
	span.SetAttributes(attribute.String("task-id", t.ID.String()))
	defer span.End()
	t.ScheduledAt = s
	buf := bytes.NewBuffer([]byte(""))
	encoder := json.NewEncoder(buf)
	if err := encoder.Encode(t); err != nil {
		return err
	}

	key := t.Key()
	value := buf.String()

	return b.Put(sctx, key, value)
}

func (b *Provider) Close() error {
	return b.Client.Close()
}

func (b *Provider) LockTask(ctx context.Context, task *schedule.Task, ttl time.Duration) error {
	_, span := tracer.Start(ctx, "lock-task")
	span.SetAttributes(attribute.String("task-id", task.ID.String()))
	defer span.End()
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
	_, span := tracer.Start(ctx, "unlock-task")
	span.SetAttributes(attribute.String("task-id", task.ID.String()))
	defer span.End()
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
