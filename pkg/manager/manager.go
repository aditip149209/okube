package manager

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/aditip149209/okube/pkg/task"
	"github.com/aditip149209/okube/pkg/worker"
	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
)

type Manager struct {
	Pending        queue.Queue
	TaskDB         map[uuid.UUID]*task.Task
	EventDB        map[uuid.UUID]*task.TaskEvent
	Workers        []string //this is an array of address strings, not worker objects.
	WorkersTaskMap map[string][]uuid.UUID
	TaskWorkersMap map[uuid.UUID]string
	LastWorker     int
}

func (m *Manager) SelectWorker() string {
	// this will act as scheduler for now
	// naive round robin
	var newWorker int
	if m.LastWorker+1 < len(m.Workers) {
		newWorker = m.LastWorker + 1
		m.LastWorker++
	} else {
		newWorker = 0
		m.LastWorker = 0
	}

	return m.Workers[newWorker]
}

func (m *Manager) UpdateTasks() {
	for _, worker := range m.Workers {
		log.Printf("Checking worker %v for task updates", worker)
		url := fmt.Sprintf("http://%tasks", worker)
		resp, err := http.Get(url)
		if err != nil {
			log.Printf("Error connecting to %v: %v\n", worker, err)

		}

		if resp.StatusCode != http.StatusOK {
			log.Printf("Error sending request: %v\n", err)
		}

		d := json.NewDecoder(resp.Body)
		var tasks []*task.Task
		err = d.Decode(&tasks)
		if err != nil {
			log.Printf("Error unmarshalling tasks: %s\n", err.Error())
		}

		for _, t := range tasks {
			log.Printf("Attempting to update task %v\n", t)
			_, ok := m.TaskDB[t.ID]
			if !ok {
				log.Printf("Task with ID %s not found\n", t.ID)
				return
			}

			if m.TaskDB[t.ID].State != t.State {
				m.TaskDB[t.ID].State = t.State
			}

			m.TaskDB[t.ID].StartTime = t.StartTime
			m.TaskDB[t.ID].EndTime = t.EndTime
			m.TaskDB[t.ID].ContainerID = t.ContainerID
		}
	}

}

func (m *Manager) SendWork() {
	if m.Pending.Len() > 0 {
		w := m.SelectWorker()

		e := m.Pending.Dequeue()

		te := e.(task.TaskEvent)

		t := te.Task
		log.Printf("Pulled %v off pending queue\n", t)

		m.EventDB[te.ID] = &te
		m.WorkersTaskMap[w] = append(m.WorkersTaskMap[w], te.Task.ID)
		m.TaskWorkersMap[t.ID] = w

		t.State = task.Scheduled

		m.TaskDB[t.ID] = &t

		data, err := json.Marshal(te)

		if err != nil {
			log.Printf("Unable to marshal task object")
		}

		url := fmt.Sprintf("http://%s/tasks", w)
		resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
		if err != nil {
			log.Printf("Error connecting to %v: %v\n", w, err)
			m.Pending.Enqueue(te)
			return
		}

		d := json.NewDecoder(resp.Body)
		if resp.StatusCode != http.StatusCreated {
			e := worker.ErrResponse{}
			err := d.Decode(&e)
			if err != nil {
				fmt.Printf("Error decoding response: %s\n", err.Error())
				return
			}
			log.Printf("Response error (%d): %s", e.HTTPStatusCode, e.Message)
			return
		}

		t = task.Task{}
		err = d.Decode(&t)
		if err != nil {
			fmt.Printf("Error decoding response: %s\n", err.Error())
			return
		}

		log.Printf("%#v\n", t)

	} else {
		log.Println("No work in the queue")
	}
}

func (m *Manager) AddTask(te task.TaskEvent) {
	m.Pending.Enqueue(te)
}

func New(workers []string) *Manager {
	TaskDB := make(map[uuid.UUID]*task.Task)
	EventDB := make(map[uuid.UUID]*task.TaskEvent)
	WorkerTaskMap := make(map[string][]uuid.UUID)
	TaskWorkerMap := make(map[uuid.UUID]string)

	for worker := range Workers {
		WorkerTaskMap[Workers[worker]] = []uuid.UUID{}
	}

	return &Manager{
		Pending:        *queue.New(),
		Workers:        workers,
		TaskDB:         TaskDB,
		EventDB:        EventDB,
		WorkersTaskMap: WorkerTaskMap,
		TaskWorkersMap: TaskWorkerMap,
	}

}
