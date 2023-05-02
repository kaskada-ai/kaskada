package table

import (
	"github.com/spf13/cobra"
)

// TableCmd represents the table command
var TableCmd = &cobra.Command{
	Use:   "table",
	Short: "A set of commands for interacting with kaskada tables",
	/*
		Long: `A longer description that spans multiple lines and likely contains examples
		and usage of using your command. For example:

		Cobra is a CLI library for Go that empowers applications.
		This application is a tool to generate the needed files
		to quickly create a Cobra application.`,
	*/
}

func init() {
	TableCmd.AddCommand(createCmd)
	TableCmd.AddCommand(deleteCmd)
	TableCmd.AddCommand(getCmd)
	TableCmd.AddCommand(loadCmd)
}

var tableName string

func initTableFlag(cmd *cobra.Command, description string) {
	cmd.Flags().StringVarP(&tableName, "table", "t", "", description)
	cmd.MarkFlagRequired("table")
}
