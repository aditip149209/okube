package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/aditip149209/okube/pkg/task"
	"github.com/google/uuid"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// EtcdConfig contains the connection settings for an EtcdStore.
type EtcdConfig struct {
	Endpoints   []string
	DialTimeout time.Duration
	Prefix      string
}

// EtcdStore is a Store implementation backed by etcd.
type EtcdStore struct {
	client *clientv3.Client
	prefix string
}

func (e *EtcdStore) managerPrefix() string {
	return fmt.Sprintf("%s/managers", e.prefix)
}

func (e *EtcdStore) managerKey(id string) string {
	return fmt.Sprintf("%s/managers/%s", e.prefix, id)
}

func (e *EtcdStore) leaderKey() string {
	return fmt.Sprintf("%s/managers/leader", e.prefix)
}

// ManagerKey returns the fully qualified etcd key for the given manager ID.
func (e *EtcdStore) ManagerKey(id string) string {
	return e.managerKey(id)
}

// LeaderKey returns the etcd key used to store the active leader metadata.
func (e *EtcdStore) LeaderKey() string {
	return e.leaderKey()
}

// Client exposes the underlying etcd client for advanced coordination features.
func (e *EtcdStore) Client() *clientv3.Client {
	return e.client
}

// NewEtcdStore creates a new EtcdStore using the supplied config.
func NewEtcdStore(cfg EtcdConfig) (*EtcdStore, error) {
	endpoints := cfg.Endpoints
	if len(endpoints) == 0 {
		endpoints = []string{"localhost:2379"}
	}

	dialTimeout := cfg.DialTimeout
	if dialTimeout == 0 {
		dialTimeout = 5 * time.Second
	}

	prefix := strings.TrimSuffix(cfg.Prefix, "/")
	if prefix != "" && !strings.HasPrefix(prefix, "/") {
		prefix = "/" + prefix
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: dialTimeout,
	})
	if err != nil {
		return nil, err
	}

	return &EtcdStore{client: cli, prefix: prefix}, nil
}

func (e *EtcdStore) tasksPrefix() string {
	return fmt.Sprintf("%s/tasks/", e.prefix)
}

// TasksPrefix exposes the tasks prefix for watch subscriptions.
func (e *EtcdStore) TasksPrefix() string {
	return e.tasksPrefix()
}

func (e *EtcdStore) taskKey(id uuid.UUID) string {
	return fmt.Sprintf("%s/tasks/%s", e.prefix, id)
}

func (e *EtcdStore) taskStateKey(id uuid.UUID) string {
	return fmt.Sprintf("%s/tasks/%s/state", e.prefix, id)
}

func (e *EtcdStore) taskWorkerKey(id uuid.UUID) string {
	return fmt.Sprintf("%s/tasks/%s/worker", e.prefix, id)
}

func (e *EtcdStore) workersPrefix() string {
	return fmt.Sprintf("%s/workers/", e.prefix)
}

func (e *EtcdStore) workerKey(id string) string {
	return fmt.Sprintf("%s/workers/%s", e.prefix, id)
}

func (e *EtcdStore) workerHeartbeatKey(id string) string {
	return fmt.Sprintf("%s/workers/%s/heartbeat", e.prefix, id)
}

// UpdateWorkerHeartbeat persists a new heartbeat timestamp for a worker.
func (e *EtcdStore) UpdateWorkerHeartbeat(ctx context.Context, workerID string, heartbeat time.Time) error {
	hbBytes, err := json.Marshal(heartbeat)
	if err != nil {
		return err
	}

	workerKey := e.workerKey(workerID)
	resp, err := e.client.Get(ctx, workerKey)
	if err != nil {
		return err
	}

	if resp.Count == 0 {
		return ErrNotFound
	}

	_, err = e.client.Txn(ctx).Then(
		clientv3.OpPut(e.workerHeartbeatKey(workerID), string(hbBytes)),
	).Commit()
	return err
}

// CreateTask stores a task and its assignment.
func (e *EtcdStore) CreateTask(ctx context.Context, t *task.Task, workerID string) error {
	if t == nil {
		return fmt.Errorf("task cannot be nil")
	}

	taskBytes, err := json.Marshal(t)
	if err != nil {
		return err
	}

	stateBytes, err := json.Marshal(t.State)
	if err != nil {
		return err
	}

	op := []clientv3.Op{
		clientv3.OpPut(e.taskKey(t.ID), string(taskBytes)),
		clientv3.OpPut(e.taskStateKey(t.ID), string(stateBytes)),
	}

	if workerID != "" {
		workerBytes, err := json.Marshal(workerID)
		if err != nil {
			return err
		}
		op = append(op, clientv3.OpPut(e.taskWorkerKey(t.ID), string(workerBytes)))
	}

	_, err = e.client.Txn(ctx).Then(op...).Commit()
	return err
}

// AssignPendingTask atomically assigns a pending task to the given worker by
// transitioning its state to Scheduled and persisting the worker ID. The
// operation succeeds only if the task is still Pending at commit time.
func (e *EtcdStore) AssignPendingTask(ctx context.Context, t *task.Task, workerID string) (bool, error) {
	if t == nil {
		return false, fmt.Errorf("task cannot be nil")
	}

	// Prepare serialized values used in the conditional transaction.
	pendingBytes, err := json.Marshal(task.Pending)
	if err != nil {
		return false, err
	}

	scheduledBytes, err := json.Marshal(task.Scheduled)
	if err != nil {
		return false, err
	}

	updated := *t
	updated.State = task.Scheduled

	taskBytes, err := json.Marshal(&updated)
	if err != nil {
		return false, err
	}

	workerBytes, err := json.Marshal(workerID)
	if err != nil {
		return false, err
	}

	txn := e.client.Txn(ctx).If(
		clientv3.Compare(clientv3.Value(e.taskStateKey(t.ID)), "=", string(pendingBytes)),
	).Then(
		clientv3.OpPut(e.taskKey(t.ID), string(taskBytes)),
		clientv3.OpPut(e.taskStateKey(t.ID), string(scheduledBytes)),
		clientv3.OpPut(e.taskWorkerKey(t.ID), string(workerBytes)),
	)

	resp, err := txn.Commit()
	if err != nil {
		return false, err
	}

	return resp.Succeeded, nil
}

// GetTask retrieves a task and its worker assignment.
func (e *EtcdStore) GetTask(ctx context.Context, id uuid.UUID) (*task.Task, string, error) {
	resp, err := e.client.Get(ctx, e.taskKey(id))
	if err != nil {
		return nil, "", err
	}

	if resp.Count == 0 {
		return nil, "", ErrNotFound
	}

	var t task.Task
	if err := json.Unmarshal(resp.Kvs[0].Value, &t); err != nil {
		return nil, "", err
	}

	stateResp, err := e.client.Get(ctx, e.taskStateKey(id))
	if err != nil {
		return nil, "", err
	}

	if stateResp.Count > 0 {
		var s task.State
		if err := json.Unmarshal(stateResp.Kvs[0].Value, &s); err != nil {
			return nil, "", err
		}
		t.State = s
	}

	workerResp, err := e.client.Get(ctx, e.taskWorkerKey(id))
	if err != nil {
		return nil, "", err
	}

	workerID := ""
	if workerResp.Count > 0 {
		if err := json.Unmarshal(workerResp.Kvs[0].Value, &workerID); err != nil {
			return nil, "", err
		}
	}

	return &t, workerID, nil
}

// UpdateTaskState updates a task's state and optional worker assignment.
func (e *EtcdStore) UpdateTaskState(ctx context.Context, t *task.Task, workerID string) error {
	if t == nil {
		return fmt.Errorf("task cannot be nil")
	}

	_, existingWorker, err := e.GetTask(ctx, t.ID)
	if err != nil {
		return err
	}

	updated := *t

	taskBytes, err := json.Marshal(&updated)
	if err != nil {
		return err
	}

	stateBytes, err := json.Marshal(updated.State)
	if err != nil {
		return err
	}

	resolvedWorker := existingWorker
	if workerID != "" {
		resolvedWorker = workerID
	}

	op := []clientv3.Op{
		clientv3.OpPut(e.taskKey(updated.ID), string(taskBytes)),
		clientv3.OpPut(e.taskStateKey(updated.ID), string(stateBytes)),
	}

	if resolvedWorker != "" {
		workerBytes, err := json.Marshal(resolvedWorker)
		if err != nil {
			return err
		}
		op = append(op, clientv3.OpPut(e.taskWorkerKey(updated.ID), string(workerBytes)))
	}

	_, err = e.client.Txn(ctx).Then(op...).Commit()
	return err
}

// ListTasks returns all tasks and their worker assignments.
func (e *EtcdStore) ListTasks(ctx context.Context) ([]TaskRecord, error) {
	resp, err := e.client.Get(ctx, e.tasksPrefix(), clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	type taskMeta struct {
		task   *task.Task
		state  *task.State
		worker string
	}

	records := make(map[string]*taskMeta)

	for _, kv := range resp.Kvs {
		segments := strings.Split(strings.TrimPrefix(string(kv.Key), "/"), "/")
		if len(segments) < 2 || segments[0] != "tasks" {
			continue
		}

		id := segments[1]
		meta, ok := records[id]
		if !ok {
			meta = &taskMeta{}
			records[id] = meta
		}

		switch len(segments) {
		case 2:
			var t task.Task
			if err := json.Unmarshal(kv.Value, &t); err != nil {
				return nil, err
			}
			meta.task = &t
		case 3:
			switch segments[2] {
			case "state":
				var s task.State
				if err := json.Unmarshal(kv.Value, &s); err != nil {
					return nil, err
				}
				meta.state = &s
			case "worker":
				var w string
				if err := json.Unmarshal(kv.Value, &w); err != nil {
					return nil, err
				}
				meta.worker = w
			}
		}
	}

	results := make([]TaskRecord, 0, len(records))
	for _, meta := range records {
		if meta.task == nil {
			continue
		}
		if meta.state != nil {
			meta.task.State = *meta.state
		}
		results = append(results, TaskRecord{Task: meta.task, WorkerID: meta.worker})
	}

	return results, nil
}

// RegisterWorker persists worker metadata and heartbeat.
func (e *EtcdStore) RegisterWorker(ctx context.Context, worker Worker) error {
	workerBytes, err := json.Marshal(worker)
	if err != nil {
		return err
	}

	heartbeatBytes, err := json.Marshal(worker.Heartbeat)
	if err != nil {
		return err
	}

	_, err = e.client.Txn(ctx).Then(
		clientv3.OpPut(e.workerKey(worker.ID), string(workerBytes)),
		clientv3.OpPut(e.workerHeartbeatKey(worker.ID), string(heartbeatBytes)),
	).Commit()
	return err
}

// ListWorkers returns all registered workers.
func (e *EtcdStore) ListWorkers(ctx context.Context) ([]Worker, error) {
	resp, err := e.client.Get(ctx, e.workersPrefix(), clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	type workerMeta struct {
		worker    *Worker
		heartbeat *time.Time
	}

	records := make(map[string]*workerMeta)

	for _, kv := range resp.Kvs {
		segments := strings.Split(strings.TrimPrefix(string(kv.Key), "/"), "/")
		if len(segments) < 2 || segments[0] != "workers" {
			continue
		}

		id := segments[1]
		meta, ok := records[id]
		if !ok {
			meta = &workerMeta{}
			records[id] = meta
		}

		switch len(segments) {
		case 2:
			var w Worker
			if err := json.Unmarshal(kv.Value, &w); err != nil {
				return nil, err
			}
			meta.worker = &w
		case 3:
			if segments[2] == "heartbeat" {
				var ts time.Time
				if err := json.Unmarshal(kv.Value, &ts); err != nil {
					return nil, err
				}
				meta.heartbeat = &ts
			}
		}
	}

	workers := make([]Worker, 0, len(records))
	for _, meta := range records {
		if meta.worker == nil {
			continue
		}
		if meta.heartbeat != nil {
			meta.worker.Heartbeat = *meta.heartbeat
		}
		workers = append(workers, *meta.worker)
	}

	return workers, nil
}
