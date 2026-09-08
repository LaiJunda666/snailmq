package broker

import (
	"errors"
	"sync"
)

type Option func(*Broker)

func WithStoreFactory(f func() Store) Option {
	return func(b *Broker) { b.newStore = f }
}

type Broker struct {
	mu       sync.RWMutex
	closed   bool
	topics   map[string]*Topic
	newStore func() Store
}

type Topic struct {
	name      string
	partition *Partition
}

func New(opts ...Option) *Broker {
	b := &Broker{
		topics:   make(map[string]*Topic),
		newStore: newMemoryLog,
	}
	for _, o := range opts {
		o(b)
	}
	if b.newStore == nil {
		b.newStore = newMemoryLog
	}
	return b
}

func (b *Broker) CreateTopic(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return ErrClosed
	}

	if name == "" {
		return errors.New("broker: empty topic name")
	}

	if _, ok := b.topics[name]; ok {
		return ErrTopicExists
	}

	b.topics[name] = &Topic{
		name:      name,
		partition: newPartition(b.newStore()),
	}
	return nil
}

func (b *Broker) topic(name string) (*Topic, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.closed {
		return nil, ErrClosed
	}

	t, ok := b.topics[name]
	if !ok {
		return nil, ErrTopicNotFound
	}
	return t, nil
}

func (b *Broker) Publish(topic string, payload []byte) (int64, error) {
	t, err := b.topic(topic)
	if err != nil {
		return 0, err
	}

	return t.partition.Publish(payload)
}

func (b *Broker) Subscribe(topic string) (*Subscription, error) {
	t, err := b.topic(topic)
	if err != nil {
		return nil, err
	}

	return t.partition.Subscribe()
}

func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return ErrClosed
	}
	b.closed = true

	for _, t := range b.topics {
		t.partition.close()
	}

	b.topics = make(map[string]*Topic)

	return nil
}
