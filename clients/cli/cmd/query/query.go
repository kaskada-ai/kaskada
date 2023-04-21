package query

import (
	"github.com/spf13/cobra"
)

// QueryCmd represents the query command
var QueryCmd = &cobra.Command{
	Use:   "query",
	Short: "A set of commands for running queries on kaskada",
	/*
		Long: `A longer description that spans multiple lines and likely contains examples
		and usage of using your command. For example:

		Cobra is a CLI library for Go that empowers applications.
		This application is a tool to generate the needed files
		to quickly create a Cobra application.`,
	*/
}

func init() {
	QueryCmd.AddCommand(runCmd)
}
