package schedule

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/0xa1-red/an-dagda/backend"
	"github.com/google/uuid"
)

type Task struct {
	ID          uuid.UUID
	Topic       string
	Data        string
	ScheduledAt time.Time
}

func NewTask(topic, data string) *Task {
	task := Task{
		ID:    uuid.New(),
		Topic: topic,
		Data:  data,
	}

	return &task
}

func (t *Task) Schedule(b backend.Provider, s time.Time) error {
	t.ScheduledAt = s
	buf := bytes.NewBuffer([]byte(""))
	encoder := json.NewEncoder(buf)
	if err := encoder.Encode(t); err != nil {
		return err
	}

	key := fmt.Sprintf("schedule/%d/%s", s.UnixNano(), t.ID.String())
	value := buf.String()

	return b.Put(key, value)
}
