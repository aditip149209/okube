package worker

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/aditip149209/okube/pkg/task"
	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
)

type Worker struct {
	Name      string
	Queue     queue.Queue
	Db        map[uuid.UUID]*task.Task
	TaskCount int
	stats     *Stats
}

func (w *Worker) CollectStats() {
	for {
		log.Println("Collecting stats")
		w.stats = GetStats()
		w.stats.Taskcount = w.TaskCount
		time.Sleep(5 * time.Second)
	}
}

func (w *Worker) GetTasks() []*task.Task {
	tasks := make([]*task.Task, 0, len(w.Db))

	for _, task := range w.Db {
		tasks = append(tasks, task)
	}

	return tasks
}

func (w *Worker) RunTask() task.DockerResult {
	t := w.Queue.Dequeue()
	if t == nil {
		log.Println("No tasks in queue")
		return task.DockerResult{Error: nil}
	}

	taskQueued, ok := t.(task.Task)
	if ok {
		taskPersisted := w.Db[taskQueued.ID]
		if taskPersisted == nil {
			taskPersisted = &taskQueued
			w.Db[taskQueued.ID] = &taskQueued
		}

		var result task.DockerResult
		if task.ValidStateTransition(taskPersisted.State, taskQueued.State) {
			switch taskQueued.State {
			case task.Scheduled:
				result = w.StartTask(taskQueued)
			case task.Completed:
				result = w.StopTask(taskQueued)
			default:
				result.Error = errors.New("We should not get here")
			}
		} else {
			err := fmt.Errorf("Invalid transition from %v to %v", taskPersisted.State, taskQueued.State)
			result.Error = err
		}
		return result
	} else {
		return task.DockerResult{Error: errors.New("Malformed task in queue")}
	}
}

func (w *Worker) StartTask(t task.Task) task.DockerResult {
	t.StartTime = time.Now().UTC()
	config := task.NewConfig(&t)
	d := task.NewDocker(config)

	result := d.Run()

	if result.Error != nil {
		log.Printf("Err running task %v: %v\n", t.ID, result.Error)
		t.State = task.Failed
		w.Db[t.ID] = &t
		return result
	}

	t.ContainerID = result.ContainerId
	t.State = task.Running
	w.Db[t.ID] = &t

	return result

}

func (w *Worker) StopTask(t task.Task) task.DockerResult {
	storedTask, ok := w.Db[t.ID]
	if !ok {
		return task.DockerResult{Error: errors.New("This task doesnt exist so it cannot be stopped")}
	}

	containerId := storedTask.ContainerID

	config := task.NewConfig(&t)
	d := task.NewDocker(config)

	result := d.Stop(containerId)
	if result.Error != nil {
		log.Printf("Error stopping container %v: %v\n", t.ContainerID, result.Error)
	}

	t.EndTime = time.Now().UTC()
	t.State = task.Completed
	w.Db[t.ID] = &t
	log.Printf("Stopped and removed container %v for task %v\n", t.ContainerID, t.ID)
	return result
}

func (w *Worker) AddTask(t task.Task) {
	w.Queue.Enqueue(t)
}
