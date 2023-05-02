package materialization

import (
	"github.com/spf13/cobra"
)

// MaterializationCmd represents the materialization command
var MaterializationCmd = &cobra.Command{
	Use:   "materialization",
	Short: "A set of commands for interacting with kaskada materializations",
	/*
		Long: `A longer description that spans multiple lines and likely contains examples
		and usage of using your command. For example:

		Cobra is a CLI library for Go that empowers applications.
		This application is a tool to generate the needed files
		to quickly create a Cobra application.`,
	*/
}

func init() {
	MaterializationCmd.AddCommand(createCmd)
	MaterializationCmd.AddCommand(deleteCmd)
	MaterializationCmd.AddCommand(getCmd)
}

var materializationName string
func initMaterializationFlag(cmd *cobra.Command, description string) {
	cmd.Flags().StringVarP(&materializationName, "materialization", "m", "", description)
	cmd.MarkFlagRequired("materialization")
}
