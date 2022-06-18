package api

import (
	"encoding/json"
	"time"
)

type ScheduleRequest struct {
	Message     string    `json:"message"`
	Channel     string    `json:"channel"`
	ScheduledAt time.Time `json:"scheduled_at"`
}

type Timestamp struct {
	time.Time
}

func (s *ScheduleRequest) UnmarshalJSON(d []byte) error {
	m := make(map[string]interface{})
	if err := json.Unmarshal(d, &m); err != nil {
		return err
	}

	s.Message = m["message"].(string)
	s.Channel = m["channel"].(string)

	ts, err := time.Parse(time.RFC3339, m["scheduled_at"].(string))
	if err != nil {
		return err
	}
	s.ScheduledAt = ts

	return nil
}

type ScheduleResponse struct {
	Status string
	Error  string
}
