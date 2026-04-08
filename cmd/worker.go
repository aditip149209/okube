/*
Copyright © 2026 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/aditip149209/okube/pkg/store"
	"github.com/aditip149209/okube/pkg/task"
	"github.com/aditip149209/okube/pkg/utils"
	"github.com/aditip149209/okube/pkg/worker"
	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
)

// workerCmd represents the worker command
var workerCmd = &cobra.Command{
	Use:   "worker",
	Short: "Worker command to operate a Cube worker node.",
	Long: `cube worker command.
	The worker runs tasks and responds to the manager's requests about task state.`,
	Run: func(cmd *cobra.Command, args []string) {
		host, _ := cmd.Flags().GetString("host")
		port, _ := cmd.Flags().GetInt("port")
		name, _ := cmd.Flags().GetString("name")
		managerHost, _ := cmd.Flags().GetString("manager-host")
		managerPort, _ := cmd.Flags().GetInt("manager-port")
		advertiseAddr, _ := cmd.Flags().GetString("advertise-address")

		workerID := name
		if workerID == "" {
			workerID = fmt.Sprintf("worker-%s", uuid.New().String())
		}

		// Determine the address the manager uses to reach this worker.
		registerIP := advertiseAddr
		if registerIP == "" {
			detected, err := utils.GetLANIP()
			if err != nil {
				log.Printf("Warning: could not auto-detect LAN IP, falling back to host flag: %v", err)
				registerIP = host
			} else {
				registerIP = detected
			}
		}

		workerAddress := fmt.Sprintf("%s:%d", registerIP, port)
		managerAddress := fmt.Sprintf("%s:%d", managerHost, managerPort)
		log.Println("Starting worker.")
		w := worker.Worker{
			Name:  workerID,
			Queue: *queue.New(),
			Db:    make(map[uuid.UUID]*task.Task),
		}

		ctx := context.Background()
		if err := worker.RegisterWithManager(ctx, managerAddress, store.Worker{ID: workerID, Address: workerAddress}); err != nil {
			log.Fatalf("Failed to register worker %s: %v", workerID, err)
		}
		go worker.StartHeartbeat(ctx, managerAddress, workerID, 10*time.Second)

		api := worker.Api{Address: host, Port: port, Worker: &w}
		go w.RunTasks()
		go w.CollectStats()
		go w.UpdateTasks()
		log.Printf("Starting worker API on http://%s:%d", host, port)
		api.Start()
	},
}

func init() {
	rootCmd.AddCommand(workerCmd)
	workerCmd.Flags().StringP("host", "H", "0.0.0.0", "Hostname or IP address")
	workerCmd.Flags().IntP("port", "p", 5556, "Port on which to listen")
	workerCmd.Flags().StringP("name", "n", fmt.Sprintf("worker-%s", uuid.New().String()), "Name of the worker")
	workerCmd.Flags().String("manager-host", "localhost", "Manager host to register with")
	workerCmd.Flags().Int("manager-port", 5556, "Manager port to register with")
	workerCmd.Flags().String("advertise-address", "", "IP address to advertise to the manager (auto-detected if empty)")
	workerCmd.Flags().StringP("dbtype", "d", "memory", "Type of datastore to use for tasks (\"memory\" or \"persistent\")")
}
