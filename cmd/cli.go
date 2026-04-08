package cmd

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/aditip149209/okube/pkg/cli"
	"github.com/aditip149209/okube/pkg/manifest"
	"github.com/aditip149209/okube/pkg/task"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
)

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Submit a new task to the cluster.",
	Long: `Submit a new container task to the okube cluster.
The task definition is provided as a JSON file via the --filename flag.
The CLI contacts any configured manager endpoint and the request is
automatically forwarded to the current leader.`,
	Run: func(cmd *cobra.Command, args []string) {
		filename, _ := cmd.Flags().GetString("filename")
		if filename == "" {
			fmt.Fprintln(os.Stderr, "Error: --filename is required")
			os.Exit(1)
		}

		data, err := os.ReadFile(filename)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading file %s: %v\n", filename, err)
			os.Exit(1)
		}

		var te task.TaskEvent
		if err := json.Unmarshal(data, &te); err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing task event JSON: %v\n", err)
			os.Exit(1)
		}

		if te.Task.ID == uuid.Nil {
			te.Task.ID = uuid.New()
		}
		if te.ID == uuid.Nil {
			te.ID = uuid.New()
		}

		client := cli.NewClient(managerEndpoints())
		resp, err := client.Do(http.MethodPost, "/tasks", te)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

		body, _ := cli.ReadBody(resp)
		if resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusOK {
			fmt.Println("Task submitted successfully.")
			// Pretty-print the response.
			var out map[string]interface{}
			if json.Unmarshal([]byte(body), &out) == nil {
				pretty, _ := json.MarshalIndent(out, "", "  ")
				fmt.Println(string(pretty))
			} else {
				fmt.Println(body)
			}
		} else {
			fmt.Fprintf(os.Stderr, "Failed to submit task (HTTP %d): %s\n", resp.StatusCode, body)
			os.Exit(1)
		}
	},
}

var stopCmd = &cobra.Command{
	Use:   "stop [task-id]",
	Short: "Stop a running task.",
	Long: `Stop a task by its UUID. The request is forwarded to the
current leader which coordinates the stop with the assigned worker.`,
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		taskID := args[0]
		if _, err := uuid.Parse(taskID); err != nil {
			fmt.Fprintf(os.Stderr, "Invalid task ID: %v\n", err)
			os.Exit(1)
		}

		client := cli.NewClient(managerEndpoints())
		resp, err := client.Do(http.MethodDelete, "/tasks/"+taskID, nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

		if resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusOK {
			fmt.Printf("Task %s stop requested.\n", taskID)
		} else {
			body, _ := cli.ReadBody(resp)
			fmt.Fprintf(os.Stderr, "Failed to stop task (HTTP %d): %s\n", resp.StatusCode, body)
			os.Exit(1)
		}
	},
}

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show cluster and task status.",
	Long: `Display the status of all tasks in the cluster and the
manager node that served the request. Works against any manager endpoint.`,
	Run: func(cmd *cobra.Command, args []string) {
		client := cli.NewClient(managerEndpoints())

		// Fetch manager status.
		statusResp, err := client.Do(http.MethodGet, "/status", nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error fetching status: %v\n", err)
			os.Exit(1)
		}

		var mgr struct {
			ManagerID     string `json:"manager_id"`
			Role          string `json:"role"`
			LeaderAddress string `json:"leader_address"`
		}
		if err := cli.ReadJSON(statusResp, &mgr); err != nil {
			log.Fatalf("Error decoding status: %v", err)
		}

		fmt.Println("=== Manager ===")
		fmt.Printf("  ID:     %s\n", mgr.ManagerID)
		fmt.Printf("  Role:   %s\n", mgr.Role)
		if mgr.LeaderAddress != "" {
			fmt.Printf("  Leader: %s\n", mgr.LeaderAddress)
		}
		fmt.Println()

		// Fetch tasks.
		tasksResp, err := client.Do(http.MethodGet, "/tasks", nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error fetching tasks: %v\n", err)
			os.Exit(1)
		}

		var tasks []task.Task
		if err := cli.ReadJSON(tasksResp, &tasks); err != nil {
			log.Fatalf("Error decoding tasks: %v", err)
		}

		if len(tasks) == 0 {
			fmt.Println("No tasks found.")
			return
		}

		fmt.Println("=== Tasks ===")
		tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "ID\tNAME\tSTATE\tIMAGE\tCONTAINER")
		for _, t := range tasks {
			containerID := t.ContainerID
			if len(containerID) > 12 {
				containerID = containerID[:12]
			}
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n",
				t.ID, t.Name, t.State, t.Image, containerID)
		}
		tw.Flush()
	},
}

var nodesCmd = &cobra.Command{
	Use:   "nodes",
	Short: "List worker nodes in the cluster.",
	Long: `Show all registered worker nodes and their heartbeat timestamps.
This data is read from the shared store via the manager API.`,
	Run: func(cmd *cobra.Command, args []string) {
		client := cli.NewClient(managerEndpoints())
		resp, err := client.Do(http.MethodGet, "/nodes", nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error fetching nodes: %v\n", err)
			os.Exit(1)
		}

		if resp.StatusCode != http.StatusOK {
			body, _ := cli.ReadBody(resp)
			fmt.Fprintf(os.Stderr, "Failed to list nodes (HTTP %d): %s\n", resp.StatusCode, body)
			os.Exit(1)
		}

		type nodeInfo struct {
			ID        string `json:"id"`
			Address   string `json:"address"`
			Heartbeat string `json:"heartbeat"`
		}
		var nodes []nodeInfo
		if err := cli.ReadJSON(resp, &nodes); err != nil {
			log.Fatalf("Error decoding nodes: %v", err)
		}

		if len(nodes) == 0 {
			fmt.Println("No worker nodes registered.")
			return
		}

		tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "ID\tADDRESS\tHEARTBEAT")
		for _, n := range nodes {
			fmt.Fprintf(tw, "%s\t%s\t%s\n", n.ID, n.Address, n.Heartbeat)
		}
		tw.Flush()
	},
}

// managerEndpoints returns the list of manager addresses from the persistent
// flag. It never returns an empty slice—defaults to localhost:5556.
func managerEndpoints() []string {
	raw, _ := rootCmd.PersistentFlags().GetString("manager")
	if raw == "" {
		return []string{"localhost:5556"}
	}
	parts := strings.Split(raw, ",")
	eps := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			eps = append(eps, p)
		}
	}
	if len(eps) == 0 {
		return []string{"localhost:5556"}
	}
	return eps
}

// ---------------------------------------------------------------------------
// deploy / apps / delete commands
// ---------------------------------------------------------------------------

var deployCmd = &cobra.Command{
	Use:   "deploy",
	Short: "Deploy a multi-service application from a manifest.",
	Long: `Deploy an application defined in a YAML manifest file. Services are
started in dependency order with automatic service discovery via env vars.`,
	Run: func(cmd *cobra.Command, args []string) {
		filename, _ := cmd.Flags().GetString("filename")
		if filename == "" {
			fmt.Fprintln(os.Stderr, "Error: --filename is required")
			os.Exit(1)
		}

		data, err := os.ReadFile(filename)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading file %s: %v\n", filename, err)
			os.Exit(1)
		}

		// Validate locally first.
		if _, err := manifest.ParseManifest(data); err != nil {
			fmt.Fprintf(os.Stderr, "Invalid manifest: %v\n", err)
			os.Exit(1)
		}

		client := cli.NewClient(managerEndpoints())
		fmt.Println("Deploying application...")

		resp, err := client.DoRaw(http.MethodPost, "/apps", data)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

		body, _ := cli.ReadBody(resp)

		if resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusOK {
			var result struct {
				App      string `json:"app"`
				Status   string `json:"status"`
				Services map[string]struct {
					TaskID   string `json:"task_id"`
					WorkerID string `json:"worker_id"`
					Address  string `json:"address"`
				} `json:"services"`
			}
			if json.Unmarshal([]byte(body), &result) == nil {
				fmt.Printf("\nApplication %q deployed — status: %s\n\n", result.App, result.Status)
				tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
				fmt.Fprintln(tw, "SERVICE\tADDRESS\tTASK ID\tWORKER")
				for name, info := range result.Services {
					fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", name, info.Address, info.TaskID, info.WorkerID)
				}
				tw.Flush()
			} else {
				fmt.Println(body)
			}
		} else {
			fmt.Fprintf(os.Stderr, "Deploy failed (HTTP %d): %s\n", resp.StatusCode, body)
			os.Exit(1)
		}
	},
}

var appsCmd = &cobra.Command{
	Use:   "apps",
	Short: "List deployed applications.",
	Run: func(cmd *cobra.Command, args []string) {
		client := cli.NewClient(managerEndpoints())
		resp, err := client.Do(http.MethodGet, "/apps", nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

		var apps []struct {
			Name         string            `json:"name"`
			ServiceTasks map[string]string `json:"service_tasks"`
			Status       string            `json:"status"`
		}
		if err := cli.ReadJSON(resp, &apps); err != nil {
			log.Fatalf("Error decoding apps: %v", err)
		}

		if len(apps) == 0 {
			fmt.Println("No applications deployed.")
			return
		}

		tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "APP NAME\tSTATUS\tSERVICES")
		for _, a := range apps {
			svcNames := make([]string, 0, len(a.ServiceTasks))
			for name := range a.ServiceTasks {
				svcNames = append(svcNames, name)
			}
			fmt.Fprintf(tw, "%s\t%s\t%s\n", a.Name, a.Status, strings.Join(svcNames, ", "))
		}
		tw.Flush()
	},
}

var deleteAppCmd = &cobra.Command{
	Use:   "delete [app-name]",
	Short: "Delete (teardown) a deployed application.",
	Long:  `Stop all services in the application in reverse dependency order and remove the app record.`,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		appName := args[0]
		client := cli.NewClient(managerEndpoints())
		resp, err := client.Do(http.MethodDelete, "/apps/"+appName, nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

		if resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusOK {
			fmt.Printf("Application %q teardown initiated.\n", appName)
		} else {
			body, _ := cli.ReadBody(resp)
			fmt.Fprintf(os.Stderr, "Failed to delete app (HTTP %d): %s\n", resp.StatusCode, body)
			os.Exit(1)
		}
	},
}

func init() {
	rootCmd.AddCommand(runCmd)
	rootCmd.AddCommand(stopCmd)
	rootCmd.AddCommand(statusCmd)
	rootCmd.AddCommand(nodesCmd)
	rootCmd.AddCommand(deployCmd)
	rootCmd.AddCommand(appsCmd)
	rootCmd.AddCommand(deleteAppCmd)

	runCmd.Flags().StringP("filename", "f", "", "Path to a JSON task definition file")
	deployCmd.Flags().StringP("filename", "f", "", "Path to a YAML manifest file")
}
