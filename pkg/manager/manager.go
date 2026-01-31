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

const heartbeatStaleAfter = 30 * time.Second

type Manager struct {
	Pending      queue.Queue
	Scheduler    scheduler.Scheduler
	WorkerClient WorkerCommunicator
	Store        store.Store
}

func (m *Manager) activeWorkers(ctx context.Context) ([]store.Worker, error) {
	if m.Store == nil {
		return nil, errors.New("store not configured")
	}

	workers, err := m.Store.ListWorkers(ctx)
	if err != nil {
		return nil, err
	}

	cutoff := time.Now().UTC().Add(-heartbeatStaleAfter)
	live := make([]store.Worker, 0, len(workers))
	for _, w := range workers {
		if w.Heartbeat.After(cutoff) {
			live = append(live, w)
		}
	}

	if len(live) == 0 {
		return nil, fmt.Errorf("no workers with heartbeat in the last %s", heartbeatStaleAfter)
	}

	return live, nil
}

func (m *Manager) SelectWorker(ctx context.Context, t task.Task) (*store.Worker, error) {
	if m.Store == nil {
		return nil, errors.New("store not configured")
	}

	workers, err := m.activeWorkers(ctx)
	if err != nil {
		return nil, err
	}

	workerMap := make(map[string]store.Worker)
	nodes := make([]*node.Node, 0, len(workers))
	for _, w := range workers {
		workerMap[w.ID] = w
		n := node.NewNode(w.ID, w.Address, "worker")
		nodes = append(nodes, n)
	}

	candidates := m.Scheduler.SelectCandidateNodes(t, nodes)
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no available candidates match resource request for task %v", t.ID)
	}

	scores := m.Scheduler.Score(t, candidates)
	selectedNode := m.Scheduler.Pick(scores, candidates)
	if selectedNode == nil {
		return nil, fmt.Errorf("scheduler failed to pick a worker for task %v", t.ID)
	}

	selectedWorker, ok := workerMap[selectedNode.Name]
	if !ok {
		return nil, fmt.Errorf("selected worker %s not found", selectedNode.Name)
	}

	return &selectedWorker, nil

}

func (m *Manager) updateTasks() {
	if m.Store == nil {
		log.Println("Store not configured; skipping task update")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	workers, err := m.activeWorkers(ctx)
	cancel()
	if err != nil {
		log.Printf("Error listing workers: %v", err)
		return
	}

	for _, worker := range workers {
		log.Printf("Checking worker %v for task updates", worker.ID)
		tasks, err := m.WorkerClient.FetchTasks(worker.Address)
		if err != nil {
			log.Printf("Error connecting to %v: %v\n", worker.Address, err)
			continue
		}

		for _, t := range tasks {
			log.Printf("Attempting to update task %v\n", t)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			persisted, _, err := m.Store.GetTask(ctx, t.ID)
			cancel()
			if err != nil {
				if errors.Is(err, store.ErrNotFound) {
					log.Printf("Task with ID %s not found in store\n", t.ID)
					continue
				}
				log.Printf("Error retrieving task %s from store: %v", t.ID, err)
				continue
			}

			persisted.State = t.State
			persisted.StartTime = t.StartTime
			persisted.EndTime = t.EndTime
			persisted.ContainerID = t.ContainerID

			ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
			if err := m.Store.UpdateTaskState(ctx, persisted, worker.ID); err != nil {
				log.Printf("Error updating task %s in store: %v", t.ID, err)
			}
			cancel()
		}
	}

}

func (m *Manager) SendWork() {
	if m.Pending.Len() == 0 {
		log.Println("No work in the queue")
		return
	}

	e := m.Pending.Dequeue()
	te, ok := e.(task.TaskEvent)
	if !ok {
		log.Printf("Unexpected item in queue: %T", e)
		return
	}

	log.Printf("Pulled task %v off the managers queue", te)

	if m.Store == nil {
		log.Println("Store not configured; cannot process task")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	existingTask, existingWorker, existingErr := m.Store.GetTask(ctx, te.Task.ID)
	cancel()
	if existingErr != nil && !errors.Is(existingErr, store.ErrNotFound) {
		log.Printf("Error retrieving task %s from store: %v", te.Task.ID, existingErr)
		m.Pending.Enqueue(te)
		return
	}

	if existingWorker != "" && existingTask != nil {
		if te.State == task.Completed && task.ValidStateTransition(existingTask.State, te.State) {
			m.stopTask(existingWorker, te.Task.ID.String())
			return
		}

		log.Printf("Invalid request: existing task %s is in state %v and cannot transition to the completed state\n",
			existingTask.ID.String(), existingTask.State)
		return
	}

	t := te.Task
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	w, err := m.SelectWorker(ctx, t)
	cancel()

	if err != nil {
		log.Printf("Error selecting worker for task %v: %v", t, err)
		m.Pending.Enqueue(te)
		return

	}

	newTask, errResp, err := m.WorkerClient.StartTask(w.Address, te)
	if err != nil {
		log.Printf("Error connecting to %v: %v\n", w.ID, err)
		m.Pending.Enqueue(te)
		return
	}

	if errResp != nil {
		log.Printf("Response error (%d): %s", errResp.HTTPStatusCode, errResp.Message)
		return
	}

	if newTask != nil {
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		if errors.Is(existingErr, store.ErrNotFound) {
			if err := m.Store.CreateTask(ctx, newTask, w.ID); err != nil {
				log.Printf("Error persisting task %s: %v", newTask.ID, err)
			}
		} else {
			if err := m.Store.UpdateTaskState(ctx, newTask, w.ID); err != nil {
				log.Printf("Error updating task %s: %v", newTask.ID, err)
			}
		}
		cancel()
		log.Printf("%#v\n", *newTask)
	}
}

func (m *Manager) AddTask(te task.TaskEvent) {
	m.Pending.Enqueue(te)

	if m.Store != nil {
		workerID := ""
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if _, existingWorker, err := m.Store.GetTask(ctx, te.Task.ID); err == nil {
			workerID = existingWorker
		}
		cancel()

		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		// Persist immediately so a manager restart can restore tasks even before dispatch.
		if err := m.Store.CreateTask(ctx, &te.Task, workerID); err != nil {
			log.Printf("Error persisting enqueued task %s: %v", te.Task.ID, err)
		}
		cancel()
	}
}

func New(workers []string, schedulerType string, st store.Store) *Manager {
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
		Pending:      *queue.New(),
		Scheduler:    s,
		WorkerClient: NewHTTPWorkerClient(nil),
		Store:        st,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	m.registerWorkers(ctx, workers)

	return m
}

func (m *Manager) registerWorkers(ctx context.Context, workers []string) {
	if m.Store == nil {
		return
	}

	for _, worker := range workers {
		meta := store.Worker{ID: worker, Address: worker, Heartbeat: time.Now().UTC()}
		if err := m.Store.RegisterWorker(ctx, meta); err != nil {
			log.Printf("Error registering worker %s: %v", worker, err)
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

func (a *Api) RegisterWorkerHandler(w http.ResponseWriter, r *http.Request) {
	if a.Manager.Store == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	var worker store.Worker
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&worker); err != nil {
		msg := fmt.Sprintf("Error unmarshalling body: %v", err)
		log.Print(msg)
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrResponse{HTTPStatusCode: http.StatusBadRequest, Message: msg})
		return
	}

	if worker.ID == "" || worker.Address == "" {
		msg := "worker id and address are required"
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrResponse{HTTPStatusCode: http.StatusBadRequest, Message: msg})
		return
	}

	worker.Heartbeat = time.Now().UTC()
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	if err := a.Manager.Store.RegisterWorker(ctx, worker); err != nil {
		msg := fmt.Sprintf("Error registering worker %s: %v", worker.ID, err)
		log.Print(msg)
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrResponse{HTTPStatusCode: http.StatusInternalServerError, Message: msg})
		return
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(worker)
}

func (a *Api) HeartbeatHandler(w http.ResponseWriter, r *http.Request) {
	if a.Manager.Store == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	workerID := chi.URLParam(r, "workerID")
	if workerID == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrResponse{HTTPStatusCode: http.StatusBadRequest, Message: "worker id is required"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	if err := a.Manager.Store.UpdateWorkerHeartbeat(ctx, workerID, time.Now().UTC()); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(ErrResponse{HTTPStatusCode: http.StatusNotFound, Message: "worker not registered"})
			return
		}
		msg := fmt.Sprintf("Error updating heartbeat for worker %s: %v", workerID, err)
		log.Print(msg)
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrResponse{HTTPStatusCode: http.StatusInternalServerError, Message: msg})
		return
	}

	w.WriteHeader(http.StatusNoContent)
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
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	taskToStop, workerID, err := a.Manager.Store.GetTask(ctx, tID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			log.Printf("No task with ID %v found", tID)
			w.WriteHeader(404)
			return
		}
		log.Printf("Error retrieving task %v: %v", tID, err)
		w.WriteHeader(500)
		return
	}

	te := task.TaskEvent{
		ID:        uuid.New(),
		State:     task.Completed,
		Timestamp: time.Now(),
	}

	taskCopy := *taskToStop
	taskCopy.State = task.Completed
	te.Task = taskCopy
	te.Task.RestartCount = taskToStop.RestartCount
	// Preserve the worker assignment so it can be used during stop processing.
	if workerID != "" {
		if err := a.Manager.Store.CreateTask(ctx, &te.Task, workerID); err != nil {
			log.Printf("Error persisting stop request for task %v: %v", tID, err)
		}
	}
	a.Manager.AddTask(te)

	log.Printf("Added task event %v to stop task %v\n", te.ID, taskToStop.ID)
	w.WriteHeader(204)

}

func (m *Manager) GetTasks() []*task.Task {
	if m.Store == nil {
		return []*task.Task{}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	records, err := m.Store.ListTasks(ctx)
	if err != nil {
		log.Printf("Error retrieving tasks from store: %v", err)
		return []*task.Task{}
	}

	tasks := make([]*task.Task, 0, len(records))
	for _, rec := range records {
		tasks = append(tasks, rec.Task)
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
	a.Router.Route("/workers", func(r chi.Router) {
		r.Post("/", a.RegisterWorkerHandler)
		r.Route("/{workerID}", func(r chi.Router) {
			r.Put("/heartbeat", a.HeartbeatHandler)
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
	if m.Store == nil {
		return errors.New("store not configured")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, workerID, err := m.Store.GetTask(ctx, t.ID)
	cancel()
	if err != nil {
		msg := fmt.Sprintf("Error retrieving worker for task %s: %v", t.ID, err)
		log.Println(msg)
		return errors.New(msg)
	}

	if workerID == "" {
		msg := fmt.Sprintf("No worker assigned for task %s", t.ID)
		log.Println(msg)
		return errors.New(msg)
	}

	hostPort := m.getHostPort(t.HostPorts)
	if hostPort == nil {
		msg := fmt.Sprintf("No host port found for task %s", t.ID)
		log.Println(msg)
		return errors.New(msg)
	}

	worker := strings.Split(workerID, ":")
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
	if m.Store == nil {
		log.Println("Store not configured; cannot restart task")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, workerID, err := m.Store.GetTask(ctx, t.ID)
	cancel()
	if err != nil {
		log.Printf("Error fetching task %s for restart: %v", t.ID, err)
		return
	}

	if workerID == "" {
		log.Printf("No worker assignment found for task %s; cannot restart", t.ID)
		return
	}

	t.State = task.Scheduled
	t.RestartCount++

	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	if err := m.Store.UpdateTaskState(ctx, t, workerID); err != nil {
		log.Printf("Error persisting restart state for task %s: %v", t.ID, err)
	}
	cancel()

	te := task.TaskEvent{
		ID:        uuid.New(),
		State:     task.Running,
		Timestamp: time.Now(),
		Task:      *t,
	}

	_, errResp, err := m.WorkerClient.StartTask(workerID, te)
	if err != nil {
		log.Printf("Error connecting to %v: %v", workerID, err)
		m.Pending.Enqueue(te)
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
		if m.Store == nil {
			log.Println("Store not configured; cannot persist stop")
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		t, _, err := m.Store.GetTask(ctx, id)
		cancel()
		if err != nil {
			log.Printf("Error fetching task %s to persist stop: %v", taskID, err)
			return
		}

		t.State = task.Completed
		t.EndTime = time.Now().UTC()

		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		if err := m.Store.UpdateTaskState(ctx, t, worker); err != nil {
			log.Printf("Error persisting stop for task %s: %v", taskID, err)
		}
		cancel()
	}

	log.Printf("Task %s has been scheduled to be stopped", taskID)
}
