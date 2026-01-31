package manager

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/aditip149209/okube/pkg/node"
	"github.com/aditip149209/okube/pkg/scheduler"
	"github.com/aditip149209/okube/pkg/task"
	"github.com/docker/go-connections/nat"
	"github.com/go-chi/chi"
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
	WorkerNodes    []*node.Node
	Scheduler      scheduler.Scheduler
	WorkerClient   WorkerCommunicator
}

func (m *Manager) SelectWorker(t task.Task) (*node.Node, error) {
	candidates := m.Scheduler.SelectCandidateNodes(t, m.WorkerNodes)
	if candidates == nil {
		msg := fmt.Sprintf("No available candidates match resource request for task %v", t.ID)
		err := errors.New(msg)
		return nil, err

	}

	scores := m.Scheduler.Score(t, candidates)
	selectedNode := m.Scheduler.Pick(scores, candidates)

	return selectedNode, nil

}

func (m *Manager) updateTasks() {
	for _, worker := range m.Workers {
		log.Printf("Checking worker %v for task updates", worker)
		tasks, err := m.WorkerClient.FetchTasks(worker)
		if err != nil {
			log.Printf("Error connecting to %v: %v\n", worker, err)
		}

		for _, t := range tasks {
			log.Printf("Attempting to update task %v\n", t)
			_, ok := m.TaskDB[t.ID]
			if !ok {
				log.Printf("Task with ID %s not found\n", t.ID)
				continue
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
		e := m.Pending.Dequeue()
		te := e.(task.TaskEvent)
		m.EventDB[te.ID] = &te
		log.Printf("Pulled task %v off the managers queue", te)

		taskWorker, ok := m.TaskWorkersMap[te.Task.ID]
		if ok {
			persistedTask := m.TaskDB[te.Task.ID]
			if persistedTask == nil {
				log.Printf("Task %s not found in TaskDB\n", te.Task.ID)
				return
			}
			if te.State == task.Completed && task.ValidStateTransition(persistedTask.State, te.State) {
				m.stopTask(taskWorker, te.Task.ID.String())
				return
			} else {
				log.Printf("%v", persistedTask)
				log.Printf("Invalid request: existing task %s is in state %v and cannot transition to the completed state\n",
					persistedTask.ID.String(), persistedTask.State)
				return
			}
		}

		t := te.Task
		w, err := m.SelectWorker(t)

		if err != nil {
			log.Printf("Error selecting worker for task %v: %v", t, err)
			m.Pending.Enqueue(te)
			return

		}

		// Add task to TaskDB for tracking
		m.TaskDB[t.ID] = &t
		m.WorkersTaskMap[w.Name] = append(m.WorkersTaskMap[w.Name], te.Task.ID)
		m.TaskWorkersMap[t.ID] = w.Name

		newTask, errResp, err := m.WorkerClient.StartTask(w.Name, te)
		if err != nil {
			log.Printf("Error connecting to %v: %v\n", w, err)
			m.Pending.Enqueue(te)
			return
		}

		if errResp != nil {
			log.Printf("Response error (%d): %s", errResp.HTTPStatusCode, errResp.Message)
			return
		}

		if newTask != nil {
			m.TaskDB[newTask.ID] = newTask
			log.Printf("%#v\n", *newTask)
		}

	} else {
		log.Println("No work in the queue")
	}
}

func (m *Manager) AddTask(te task.TaskEvent) {
	m.Pending.Enqueue(te)
}

func New(workers []string, schedulerType string) *Manager {
	TaskDB := make(map[uuid.UUID]*task.Task)
	EventDB := make(map[uuid.UUID]*task.TaskEvent)
	WorkerTaskMap := make(map[string][]uuid.UUID)
	TaskWorkerMap := make(map[uuid.UUID]string)

	var nodes []*node.Node

	for worker := range workers {
		WorkerTaskMap[workers[worker]] = []uuid.UUID{}
		nAPI := fmt.Sprintf("http://%v", workers[worker])
		n := node.NewNode(workers[worker], nAPI, "worker")
		nodes = append(nodes, n)
	}

	var s scheduler.Scheduler
	switch schedulerType {
	case "roundrobin":
		s = &scheduler.RoundRobin{Name: "roundrobin"}
	default:
		s = &scheduler.RoundRobin{Name: "roundrobin"}
	}

	return &Manager{
		Pending:        *queue.New(),
		Workers:        workers,
		TaskDB:         TaskDB,
		EventDB:        EventDB,
		WorkersTaskMap: WorkerTaskMap,
		TaskWorkersMap: TaskWorkerMap,
		WorkerNodes:    nodes,
		Scheduler:      s,
		WorkerClient:  NewHTTPWorkerClient(nil),
	}
}

// manager api- this is what allows the users to interact with the okube cluster. essentially, we are building a way for the end user to interact with okube
type Api struct {
	Address string
	Port    int
	Manager *Manager
	Router  *chi.Mux
}

type ErrResponse struct {
	HTTPStatusCode int    `json:"status"`
	Message        string `json:"message"`
}

func (a *Api) StartTaskHandler(w http.ResponseWriter, r *http.Request) {
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()

	te := task.TaskEvent{}

	err := d.Decode(&te)

	if err != nil {
		msg := fmt.Sprintf("Error unmarshalling body: %v\n", err)
		log.Print(msg)
		w.WriteHeader(400)
		e := ErrResponse{
			HTTPStatusCode: 400,
			Message:        msg,
		}
		json.NewEncoder(w).Encode(e)
		return
	}

	a.Manager.AddTask(te)
	log.Printf("Added task %v\n", te.Task.ID)
	w.WriteHeader(201)
	json.NewEncoder(w).Encode(te)
}

func (a *Api) GetTasksHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	json.NewEncoder(w).Encode(a.Manager.GetTasks())
}

func (a *Api) StopTaskHandler(w http.ResponseWriter, r *http.Request) {
	taskID := chi.URLParam(r, "taskID")
	if taskID == "" {
		log.Printf("No TaskID passed in request.\n")
		w.WriteHeader(400)
	}

	tID, _ := uuid.Parse(taskID)

	taskToStop, ok := a.Manager.TaskDB[tID]

	if !ok {
		log.Printf("No task with ID %v found", tID)
		w.WriteHeader(404)
	}

	te := task.TaskEvent{
		ID:        uuid.New(),
		State:     task.Completed,
		Timestamp: time.Now(),
	}

	taskCopy := *taskToStop
	taskCopy.State = task.Completed
	te.Task = taskCopy
	a.Manager.AddTask(te)

	log.Printf("Added task event %v to stop task %v\n", te.ID, taskToStop.ID)
	w.WriteHeader(204)

}

func (m *Manager) GetTasks() []*task.Task {
	tasks := []*task.Task{}
	for _, t := range m.TaskDB {
		tasks = append(tasks, t)
	}
	return tasks
}

func (a *Api) initRouter() {
	a.Router = chi.NewRouter()
	a.Router.Route("/tasks", func(r chi.Router) {
		r.Post("/", a.StartTaskHandler)
		r.Get("/", a.GetTasksHandler)
		r.Route("/{taskID}", func(r chi.Router) {
			r.Delete("/", a.StopTaskHandler)
		})
	})
}

func (a *Api) Start() {
	a.initRouter()
	http.ListenAndServe(fmt.Sprintf("%s:%d", a.Address, a.Port), a.Router)
}

func (m *Manager) UpdateTasks() {
	for {
		log.Println("Checking for task updates from workers")
		m.updateTasks()
		log.Println("Tasks update completed")
		log.Println("Sleeping for 15 seconds")
		time.Sleep(15 * time.Second)
	}
}

func (m *Manager) ProcessTasks() {
	for {
		log.Println("Processing any tasks in the queue")
		m.SendWork()
		log.Printf("Sleeping for 10 seconds")
		time.Sleep(10 * time.Second)
	}
}

func (m *Manager) getHostPort(ports nat.PortMap) *string {
	for k, _ := range ports {
		return &ports[k][0].HostPort
	}

	return nil
}

func (m *Manager) checkTaskHealth(t task.Task) error {
	log.Printf("Calling health check for task %s: %s\n", t.ID, t.HealthCheck)

	w := m.TaskWorkersMap[t.ID]

	hostPort := m.getHostPort(t.HostPorts)
	worker := strings.Split(w, ":")
	url := fmt.Sprintf("http://%s:%s%s", worker[0], *hostPort, t.HealthCheck)

	log.Printf("Calling health check for task %s: %s\n", t.ID, url)

	resp, err := http.Get(url)

	if err != nil {
		msg := fmt.Sprintf("Error connecting to health check %s", url)
		log.Println(msg)
		return errors.New(msg)
	}

	if resp.StatusCode != http.StatusOK {
		msg := fmt.Sprintf("Error health check for task %s did not return 200\n", t.ID)
		log.Println(msg)
		return errors.New(msg)
	}

	log.Printf("Task %s health check response: %v\n", t.ID, resp.StatusCode)

	return nil

}

func (m *Manager) doHealthChecks() {
	for _, t := range m.GetTasks() {
		if t.State == task.Running && t.RestartCount < 3 {
			err := m.checkTaskHealth(*t)
			if err != nil {
				if t.RestartCount < 3 {
					m.restartTask(t)
				}
			}
		} else if t.State == task.Failed && t.RestartCount < 3 {
			m.restartTask(t)
		}
	}
}

func (m *Manager) restartTask(t *task.Task) {
	w := m.TaskWorkersMap[t.ID]
	t.State = task.Scheduled
	t.RestartCount++

	m.TaskDB[t.ID] = t

	te := task.TaskEvent{
		ID:        uuid.New(),
		State:     task.Running,
		Timestamp: time.Now(),
		Task:      *t,
	}

	_, errResp, err := m.WorkerClient.StartTask(w, te)
	if err != nil {
		log.Printf("Error connecting to %v: %v", w, err)
		m.Pending.Enqueue(t)
		return
	}

	if errResp != nil {
		log.Printf("Response error (%d): %s", errResp.HTTPStatusCode, errResp.Message)
		return
	}

	log.Printf("%v\n", t)

}

func (m *Manager) DoHealthChecks() {
	for {
		log.Println("Performing task health check")
		m.doHealthChecks()
		log.Println("Task health checks completed")
		log.Println("Sleeping for 60 seconds")
		time.Sleep(60 * time.Second)
	}
}

func (m *Manager) stopTask(worker string, taskID string) {
	err := m.WorkerClient.StopTask(worker, taskID)
	if err != nil {
		log.Printf("Error sending request: %v\n", err)
		return
	}

	log.Printf("Task %s has been scheduled to be stopped", taskID)
}
