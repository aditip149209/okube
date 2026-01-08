package worker

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/aditip149209/okube/pkg/task"
	"github.com/go-chi/chi"
	"github.com/google/uuid"
)

type ErrResponse struct {
	HTTPStatusCode int    `json:"status"`
	Message        string `json:"message"`
}

func (a *Api) StartTaskHandler(w http.ResponseWriter, r *http.Request) {
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()

	te := task.TaskEvent{}

	err := d.Decode(te)

	if err != nil {
		msg := fmt.Sprintf("Error unmarshalling body: %v\n", err)
		log.Printf(msg)
		w.WriteHeader(400)
		e := ErrResponse{
			HTTPStatusCode: 400,
			Message:        msg,
		}
		json.NewEncoder(w).Encode(e)
		return
	}

	a.Worker.AddTask(te.Task)
	log.Printf("Added task %v\n", te.Task.ID)
	w.WriteHeader(201)
	json.NewEncoder(w).Encode(te.Task)

}

func (a *Api) StopTaskHandler(w http.ResponseWriter, r *http.Request) {
	taskID := chi.URLParam(r, "taskID")
	if taskID == "" {
		log.Printf("No task id passed in request\n")
		w.WriteHeader(400)
	}

	tID, _ := uuid.Parse(taskID)
	_, ok := a.Worker.Db[tID]
	if !ok {
		log.Printf("No task with id %v found\n", tID)
		w.WriteHeader(404)
	}

	taskToStop := a.Worker.Db[tID]
	taskCopy := *taskToStop

	taskCopy.State = task.Completed
	a.Worker.AddTask(taskCopy)

	log.Printf("Added task %v to stop container %v\n", taskToStop.ID, taskToStop.ContainerID)
	w.WriteHeader(204)
}

func (a *Api) GetTaskHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	json.NewEncoder(w).Encode(a.Worker.GetTasks())
}

func (a *Api) GetStatsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	json.NewEncoder(w).Encode(a.Worker.stats)
}
