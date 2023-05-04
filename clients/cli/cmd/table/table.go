package table

import (
	"github.com/kaskada-ai/kaskada/clients/cli/api"
	"github.com/kaskada-ai/kaskada/clients/cli/utils"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/reflect/protoreflect"

	apiv1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
)

const table = "table"

// TableCmd represents the table command
var TableCmd = &cobra.Command{
	Use:   table,
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

func printTable(item protoreflect.ProtoMessage) {
	table, err := api.ProtoToTable(item)
	utils.LogAndQuitIfErrorExists(err)
	switch table.Source.Source.(type) {
	case *apiv1alpha.Source_Kaskada:
		table.Source = nil
	}

	yaml, err := utils.ProtoToYaml(table)
	utils.LogAndQuitIfErrorExists(err)
	utils.PrintSuccessf("%s", yaml)
}
