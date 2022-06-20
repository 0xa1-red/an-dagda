package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"

	"github.com/0xa1-red/an-dagda/task"
	"github.com/asynkron/protoactor-go/cluster"
	"github.com/asynkron/protoactor-go/log"
	"github.com/gorilla/mux"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	plog = log.New(log.InfoLevel, "[DAEMON][api]")
)

type Server struct {
	*http.Server

	cluster *cluster.Cluster
}

func New(cluster *cluster.Cluster) *Server {
	m := mux.NewRouter()

	m.HandleFunc("/schedule", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Content-Type") != "application/json" {
			http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
			return
		}

		var req ScheduleRequest
		decoder := json.NewDecoder(r.Body)
		if err := decoder.Decode(&req); err != nil {
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}

		preq := task.ScheduleRequest{
			Message:    req.Message,
			Channel:    req.Channel,
			ScheduleAt: timestamppb.New(req.ScheduledAt),
		}

		client := task.GetSchedulerGrainClient(cluster, task.OverseerID.String())
		pres, err := client.Schedule(&preq)
		if err != nil {
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}

		var statusText string
		switch pres.GetStatus() {
		case task.Status_Error:
			statusText = "Error"
		case task.Status_OK:
			statusText = "OK"
		}

		buf := new(bytes.Buffer)
		encoder := json.NewEncoder(buf)
		if err := encoder.Encode(ScheduleResponse{Status: statusText}); err != nil {
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}

		if _, err := w.Write(buf.Bytes()); err != nil {
			plog.Error("Error writing response", log.Error(err))
		}
	})

	s := http.Server{
		Handler: m,
		Addr:    "0.0.0.0:80",
	}

	ss := Server{
		Server: &s,
	}

	go func(s *Server) {
		if err := s.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			plog.Error("HTTP server error", log.Error(err))
			return
		}
	}(&ss)

	return &ss
}

func (s *Server) Stop() {
	if err := s.Server.Shutdown(context.Background()); err != nil {
		plog.Error("Error shutting down HTTP server", log.Error(err))
	}
}
