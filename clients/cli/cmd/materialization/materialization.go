package materialization

import (
	"github.com/kaskada-ai/kaskada/clients/cli/api"
	"github.com/kaskada-ai/kaskada/clients/cli/utils"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/reflect/protoreflect"

	apiv1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
)

const materialization = "materialization"

// MaterializationCmd represents the materialization command
var MaterializationCmd = &cobra.Command{
	Use:   materialization,
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
	MaterializationCmd.AddCommand(listCmd)
}

func getMaterializationFromItem(item protoreflect.ProtoMessage) *apiv1alpha.Materialization {
	materialization, err := api.ProtoToMaterialization(item)
	utils.LogAndQuitIfErrorExists(err)
	return materialization
}

func printMaterialization(item protoreflect.ProtoMessage) {
	materialization := getMaterializationFromItem(item)
	yaml, err := utils.ProtoToYaml(materialization)
	utils.LogAndQuitIfErrorExists(err)
	utils.PrintSuccessf("%s", yaml)
}
