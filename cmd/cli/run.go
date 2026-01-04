package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

func init() {
	//register run as child of okube
	rootCmd.AddCommand(runCmd)

	//flags
	runCmd.Flags().StringP("image", "i", "", "Container image to run(required)")
	runCmd.Flags().IntP("replicas", "r", 1, "Number of copies to start")

	runCmd.MarkFlagRequired("image")
}

var runCmd = &cobra.Command{
	Use:     "run",
	Short:   "start a new task on the cluster",
	Example: "okube run --image nginx --replicas 3",
	Run: func(cmd *cobra.Command, args []string) {
		image, _ := cmd.Flags().GetString("image")
		replicas, _ := cmd.Flags().GetInt("replicas")

		fmt.Println("Sending request to control plane")

		//logic for calling http post function to api server
		fmt.Println("this is the image", image)
		fmt.Println("this is the replica count", replicas)

	},
}
