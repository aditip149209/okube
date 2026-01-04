package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "okube",
	Short: "a network aware container orchestrator",
	Long:  "A k3s style network aware container orchestrator, designed for on premise server architectures",
}

func main() {

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

}
