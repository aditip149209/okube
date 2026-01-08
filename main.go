package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aditip149209/okube/pkg/task"
	"github.com/aditip149209/okube/pkg/worker"
	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
)

func runTasks(w *worker.Worker) {
	for {
		if w.Queue.Len() != 0 {
			result := w.RunTask()
			if result.Error != nil {
				log.Printf("Error running task: %v\n", result.Error)
			}
		} else {
			log.Printf("No tasks to process currently\n")
		}
		log.Println("Sleeping for 10 seconds")
		time.Sleep(time.Second * 10)
	}
}

func main() {

	host := os.Getenv("CUBE_HOST")
	port := 3002

	fmt.Println("Starting Cube worker")

	w := worker.Worker{
		Queue: *queue.New(),
		Db:    make(map[uuid.UUID]*task.Task),
	}

	api := worker.Api{Address: host, Port: port, Worker: &w}

	go runTasks(&w)
	go w.CollectStats()
	api.Start()

	// db := make(map[uuid.UUID]*task.Task)
	// w1 := worker.Worker{
	// 	Queue: *queue.New(),
	// 	Db:    db,
	// }

	// t1 := task.Task{
	// 	ID:    uuid.New(),
	// 	Name:  "test-container-1",
	// 	State: task.Scheduled,
	// 	Image: "strm/helloworld-http",
	// }

	// w2 := worker.Worker{
	// 	Queue: *queue.New(),
	// 	Db:    db,
	// }

	// t2 := task.Task{
	// 	ID:    uuid.New(),
	// 	Name:  "test-container2",
	// 	State: task.Scheduled,
	// 	Image: "strm/helloworld-http",
	// }

	// fmt.Println("starting task")

	// w2.AddTask(t2)
	// w1.AddTask(t1)

	// res1 := w1.RunTask()
	// if res1.Error != nil {
	// 	panic(res1.Error)
	// }

	// fmt.Printf("Task %s is running in container %s\n", t1.ID, t1.ContainerID)
	// fmt.Println("Sleepy time")
	// time.Sleep(time.Second * 3)

	// fmt.Printf("stopping task %s\n", t1.ID)
	// t1.State = task.Completed
	// w1.AddTask(t1)
	// res1 = w1.RunTask()

	// // w2.AddTask(t1)

	// res2 := w2.RunTask()
	// fmt.Println("print sth")
	// t2.State = task.Completed
	// w2.AddTask(t2)
	// res2 = w2.RunTask()
	// if res2.Error != nil {
	// 	panic(res2.Error)
	// } else {
	// 	fmt.Printf("all tasks ended yay")
	// }

}

// func createContainer() (*task.Docker, *task.DockerResult) {
// 	c := task.Config{
// 		Name:  "test-container-1",
// 		Image: "postgres:13",
// 		Env: []string{
// 			"POSTGRES_USER=cube",
// 			"POSTGRES_PASSWORD=secret",
// 		},
// 	}

// 	dc, _ := client.NewClientWithOpts(client.FromEnv)
// 	d := task.Docker{
// 		Client: dc,
// 		Config: c,
// 	}

// 	result := d.Run()
// 	if result.Error != nil {
// 		fmt.Printf("%v\n", result.Error)
// 		return nil, nil
// 	}

// 	fmt.Printf(
// 		"Container %s is running with config %v\n", result.ContainerId, c,
// 	)

// 	return &d, &result
// }

// func stopContainer(d *task.Docker, id string) *task.DockerResult {
// 	result := d.Stop(id)
// 	if result.Error != nil {
// 		fmt.Printf("%v\n", result.Error)
// 		return nil
// 	}

// 	fmt.Printf("Container %s has been stopped and removed\n", result.ContainerId)
// 	return &result

// }
