package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/aditip149209/okube/pkg/manager"
	"github.com/aditip149209/okube/pkg/task"
	"github.com/aditip149209/okube/pkg/worker"
	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
)

func main() {

	whost := os.Getenv("CUBE_WORKER_HOST")
	if whost == "" {
		whost = "localhost"
	}

	wportStr := os.Getenv("CUBE_WORKER_PORT")
	if wportStr == "" {
		wportStr = "5555"
	}
	wport, _ := strconv.Atoi(wportStr)

	mhost := os.Getenv("CUBE_MANAGER_HOST")
	if mhost == "" {
		mhost = "localhost"
	}

	mportStr := os.Getenv("CUBE_MANAGER_PORT")
	if mportStr == "" {
		mportStr = "5556"
	}
	mport, _ := strconv.Atoi(mportStr)

	fmt.Println("Starting Cube worker")

	w := worker.Worker{
		Queue: *queue.New(),
		Db:    make(map[uuid.UUID]*task.Task),
	}

	wapi := worker.Api{Address: whost, Port: wport, Worker: &w}

	go w.RunTasks()
	go w.CollectStats()
	go wapi.Start()

	workers := []string{fmt.Sprintf("%v:%v", whost, wport)}
	m := manager.New(workers)

	mapi := manager.Api{Address: mhost, Port: mport, Manager: m}

	go m.ProcessTasks()
	go m.UpdateTasks()
	go mapi.Start()

	for i := 0; i < 3; i++ {
		t := task.Task{
			ID:    uuid.New(),
			Name:  fmt.Sprintf("test-container-%d", i),
			State: task.Scheduled,
			Image: "strm/helloworld-http",
		}
		te := task.TaskEvent{
			ID:    uuid.New(),
			State: task.Running,
			Task:  t,
		}
		m.AddTask(te)
		m.SendWork()
	}

	go func() {
		for {
			m.UpdateTasks()
			time.Sleep(15 * time.Second)
		}
	}()

	for {
		for _, t := range m.TaskDB {
			fmt.Printf("[Manager] Task: id: %s, state: %d\n", t.ID, t.State)
		}
		time.Sleep(10 * time.Second)
	}

}
