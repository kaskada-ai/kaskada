package sync

import (
	"github.com/spf13/cobra"
)

// SyncCmd represents the sync command
var SyncCmd = &cobra.Command{
	Use:   "sync",
	Short: "A set of commands for interacting with kaskada resources as code",
	/*
		Long: `A longer description that spans multiple lines and likely contains examples
		and usage of using your command. For example:

		Cobra is a CLI library for Go that empowers applications.
		This application is a tool to generate the needed files
		to quickly create a Cobra application.`,
	*/
}

func init() {
	SyncCmd.AddCommand(exportCmd)
	SyncCmd.AddCommand(planCmd)
	SyncCmd.AddCommand(applyCmd)
}

