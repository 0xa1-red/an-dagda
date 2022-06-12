package backend

import "context"

type Provider interface {
	Put(ctx context.Context, key, value string) error
	Delete(ctx context.Context, key string) error
	Close() error
}
