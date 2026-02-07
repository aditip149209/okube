/*
Copyright © 2026 NAME HERE <EMAIL ADDRESS>

*/
package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "okube",
	Short: "okube – a container orchestrator with HA-aware CLI",
	Long: `okube is a lightweight container orchestrator.

The CLI connects to one or more manager endpoints and automatically
follows leader redirects so it works correctly even during failover.

Use --manager to specify a comma-separated list of manager addresses
(defaults to localhost:5556).

Examples:
  okube --manager mgr1:5556,mgr2:5557 status
  okube run -f task.json
  okube stop <task-id>
  okube nodes`,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringP("manager", "m", "",
		"Comma-separated list of manager endpoints (host:port). "+
			"Env: OKUBE_MANAGERS. Default: localhost:5556")

	// Bind to environment variable as fallback.
	if envMgr := os.Getenv("OKUBE_MANAGERS"); envMgr != "" {
		rootCmd.PersistentFlags().Lookup("manager").DefValue = envMgr
		_ = rootCmd.PersistentFlags().Set("manager", envMgr)
	}
}


