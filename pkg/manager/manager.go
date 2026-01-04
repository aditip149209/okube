package manager

import (
	"fmt"

	"github.com/aditip149209/okube/pkg/task"
	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
)

type Manager struct {
	Pending        queue.Queue
	TaskDB         map[string][]*task.Task
	EventDB        map[string][]*task.TaskEvent
	Workers        []string
	WorkersTaskMap map[string][]uuid.UUID
	TaskWorkersMap map[uuid.UUID]string
}

func (m *Manager) SelectWorker() {
	fmt.Println("This will select a worker based on the scheduler?")
}

func (m *Manager) UpdateTasks() {
	fmt.Println("This will collect the data about all the tasks running on all the workers")
}

func (m *Manager) SendWork() {
	fmt.Println("This will send work to workers")
}
