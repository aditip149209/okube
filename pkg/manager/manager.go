package manager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/aditip149209/okube/pkg/node"
	"github.com/aditip149209/okube/pkg/scheduler"
	"github.com/aditip149209/okube/pkg/store"
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
	Store          store.Store
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

			if m.Store != nil {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				if err := m.Store.UpdateTaskState(ctx, m.TaskDB[t.ID], worker); err != nil {
					log.Printf("Error updating task %s in store: %v", t.ID, err)
				}
				cancel()
			}
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

			if m.Store != nil {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				if err := m.Store.CreateTask(ctx, newTask, w.Name); err != nil {
					log.Printf("Error persisting task %s: %v", newTask.ID, err)
				}
				cancel()
			}
			log.Printf("%#v\n", *newTask)
		}

	} else {
		log.Println("No work in the queue")
	}
}

func (m *Manager) AddTask(te task.TaskEvent) {
	m.Pending.Enqueue(te)

	if m.Store != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		// Persist immediately so a manager restart can restore tasks even before dispatch.
		workerID := m.TaskWorkersMap[te.Task.ID]
		if err := m.Store.CreateTask(ctx, &te.Task, workerID); err != nil {
			log.Printf("Error persisting enqueued task %s: %v", te.Task.ID, err)
		}
		cancel()
	}
}

func New(workers []string, schedulerType string, st store.Store) *Manager {
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
	case "epvm":
		s = &scheduler.Epvm{Name: "epvm"}
	default:
		s = &scheduler.RoundRobin{Name: "roundrobin"}
	}

	m := &Manager{
		Pending:        *queue.New(),
		Workers:        workers,
		TaskDB:         TaskDB,
		EventDB:        EventDB,
		WorkersTaskMap: WorkerTaskMap,
		TaskWorkersMap: TaskWorkerMap,
		WorkerNodes:    nodes,
		Scheduler:      s,
		WorkerClient:   NewHTTPWorkerClient(nil),
		Store:          st,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	m.registerWorkers(ctx)
	m.loadPersistedState(ctx)

	return m
}

func (m *Manager) registerWorkers(ctx context.Context) {
	if m.Store == nil {
		return
	}

	for _, worker := range m.Workers {
		meta := store.Worker{ID: worker, Address: worker, Heartbeat: time.Now().UTC()}
		if err := m.Store.RegisterWorker(ctx, meta); err != nil {
			log.Printf("Error registering worker %s: %v", worker, err)
		}
	}
}

func (m *Manager) loadPersistedState(ctx context.Context) {
	if m.Store == nil {
		return
	}

	tasks, err := m.Store.ListTasks(ctx)
	if err != nil {
		log.Printf("Error loading tasks from store: %v", err)
		return
	}

	for _, rec := range tasks {
		m.TaskDB[rec.Task.ID] = rec.Task
		if rec.WorkerID != "" {
			if _, ok := m.WorkersTaskMap[rec.WorkerID]; !ok {
				m.WorkersTaskMap[rec.WorkerID] = []uuid.UUID{}
			}
			m.WorkersTaskMap[rec.WorkerID] = append(m.WorkersTaskMap[rec.WorkerID], rec.Task.ID)
			m.TaskWorkersMap[rec.Task.ID] = rec.WorkerID
		}
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
	if hostPort == nil {
		msg := fmt.Sprintf("No host port found for task %s", t.ID)
		log.Println(msg)
		return errors.New(msg)
	}

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
			// Only check health if a health check URL is defined
			if t.HealthCheck != "" {
				err := m.checkTaskHealth(*t)
				if err != nil {
					if t.RestartCount < 3 {
						m.restartTask(t)
					}
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

	if m.Store != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := m.Store.UpdateTaskState(ctx, t, w); err != nil {
			log.Printf("Error persisting restart state for task %s: %v", t.ID, err)
		}
		cancel()
	}

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
	if id, err := uuid.Parse(taskID); err == nil {
		if t, ok := m.TaskDB[id]; ok {
			t.State = task.Completed
			t.EndTime = time.Now().UTC()
			m.TaskDB[id] = t

			if m.Store != nil {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				if err := m.Store.UpdateTaskState(ctx, t, worker); err != nil {
					log.Printf("Error persisting stop for task %s: %v", taskID, err)
				}
				cancel()
			}
		}
	}

	log.Printf("Task %s has been scheduled to be stopped", taskID)
}
