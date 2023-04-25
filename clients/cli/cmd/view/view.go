package view

import (
	"github.com/spf13/cobra"
)

// ViewCmd represents the view command
var ViewCmd = &cobra.Command{
	Use:   "view",
	Short: "A set of commands for interacting with kaskada views",
	/*
		Long: `A longer description that spans multiple lines and likely contains examples
		and usage of using your command. For example:

		Cobra is a CLI library for Go that empowers applications.
		This application is a tool to generate the needed files
		to quickly create a Cobra application.`,
	*/
}

func init() {
	ViewCmd.AddCommand(deleteCmd)
}

var view string
func initViewFlag(cmd *cobra.Command, description string) {
	cmd.Flags().StringVarP(&view, "view", "v", "", description)
	cmd.MarkFlagRequired("view")
}
