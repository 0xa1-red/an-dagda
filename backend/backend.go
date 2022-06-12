package backend

type Provider interface {
	Put(key, value string) error
	Close() error
}
