package store

import (
	"context"
	"errors"
	"time"

	"github.com/aditip149209/okube/pkg/appgroup"
	"github.com/aditip149209/okube/pkg/task"
	"github.com/aditip149209/okube/pkg/topology"
	"github.com/google/uuid"
)

// ErrNotFound is returned when a requested item does not exist in the store.
var ErrNotFound = errors.New("store: not found")

// Worker represents a worker registration persisted in the store.
type Worker struct {
	ID        string    `json:"id"`
	Address   string    `json:"address"`
	Heartbeat time.Time `json:"heartbeat"`
}

// TaskRecord includes a task along with its latest worker assignment.
type TaskRecord struct {
	Task     *task.Task `json:"task"`
	WorkerID string     `json:"worker_id,omitempty"`
}

// Store defines the contract for persisting tasks and workers.
type Store interface {
	CreateTask(ctx context.Context, t *task.Task, workerID string) error
	GetTask(ctx context.Context, id uuid.UUID) (*task.Task, string, error)
	GetNodeOfTask(ctx context.Context, id uuid.UUID) (string, error)
	UpdateTaskState(ctx context.Context, t *task.Task, workerID string) error
	ListTasks(ctx context.Context) ([]TaskRecord, error)
	RegisterWorker(ctx context.Context, worker Worker) error
	ListWorkers(ctx context.Context) ([]Worker, error)
	UpdateWorkerHeartbeat(ctx context.Context, workerID string, heartbeat time.Time) error

	// AppGroup persistence
	CreateAppGroup(ctx context.Context, ag *appgroup.AppGroup) error
	GetAppGroup(ctx context.Context, appID string) (*appgroup.AppGroup, error)
	ListAppGroups(ctx context.Context) ([]*appgroup.AppGroup, error)

	// Network topology persistence
	SaveNetworkTopology(ctx context.Context, nt *topology.NetworkTopology) error
	GetNetworkTopology(ctx context.Context) (*topology.NetworkTopology, error)
}
