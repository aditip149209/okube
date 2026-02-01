/*
Copyright © 2026 NAME HERE <EMAIL ADDRESS>
*/
package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/aditip149209/okube/cmd"
	"github.com/aditip149209/okube/pkg/manager"
	"github.com/aditip149209/okube/pkg/store"
	"github.com/aditip149209/okube/pkg/task"
	"github.com/aditip149209/okube/pkg/worker"
	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
)

func main() {
	cmd.Execute()

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

	fmt.Println("Starting Cube workers")

	w1 := worker.Worker{
		Queue: *queue.New(),
		Db:    make(map[uuid.UUID]*task.Task),
	}
	wapi1 := worker.Api{Address: whost, Port: wport, Worker: &w1}

	w2 := worker.Worker{
		Queue: *queue.New(),
		Db:    make(map[uuid.UUID]*task.Task),
	}
	wapi2 := worker.Api{Address: whost, Port: wport + 1, Worker: &w2}

	w3 := worker.Worker{
		Queue: *queue.New(),
		Db:    make(map[uuid.UUID]*task.Task),
	}
	wapi3 := worker.Api{Address: whost, Port: wport + 2, Worker: &w3}

	go w1.RunTasks()
	go w1.UpdateTasks()
	go wapi1.Start()

	go w2.RunTasks()
	go w2.UpdateTasks()
	go wapi2.Start()

	go w3.RunTasks()
	go w3.UpdateTasks()
	go wapi3.Start()

	workers := []string{
		fmt.Sprintf("%s:%d", whost, wport),
		fmt.Sprintf("%s:%d", whost, wport+1),
		fmt.Sprintf("%s:%d", whost, wport+2),
	}

	etcdStore, err := store.NewEtcdStore(store.EtcdConfig{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("Failed to initialize etcd store: %v", err)
	}

	m := manager.NewWithConfig(manager.Config{
		Workers:       workers,
		SchedulerType: "epvm",
		Store:         etcdStore,
		Role:          manager.ManagerRoleLeader,
	})
	mapi := manager.Api{Address: mhost, Port: mport, Manager: m}

	go m.UpdateTasks()
	go m.DoHealthChecks()

	go mapi.Start()

	log.Printf("This the manager %v", m)

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
		if err := m.AddTask(te); err != nil {
			log.Printf("Failed to enqueue task %s: %v", te.Task.ID, err)
		}
	}

	// Keep main goroutine alive
	select {}

}
