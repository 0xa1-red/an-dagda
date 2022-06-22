package backend

import (
	"context"
	"time"

	"github.com/0xa1-red/an-dagda/schedule"
)

type Provider interface {
	Put(ctx context.Context, key, value string) error
	Delete(ctx context.Context, key string) error
	Schedule(ctx context.Context, task *schedule.Task, timestamp time.Time) error
	GetCurrentTasks(ctx context.Context) ([]*schedule.Task, error)
	LockTask(ctx context.Context, task *schedule.Task, ttl time.Duration) error
	UnlockTask(ctx context.Context, task *schedule.Task) error
	Close() error
}
