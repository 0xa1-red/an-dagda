package schedule

import (
	"fmt"
	"time"

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

func (t *Task) Key() string {
	return fmt.Sprintf("schedule/%d/%s", t.ScheduledAt.UnixNano(), t.ID.String())
}
