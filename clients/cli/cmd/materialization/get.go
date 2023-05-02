package materialization

import (
	"fmt"

	"github.com/kaskada-ai/kaskada/clients/cli/api"
	"github.com/kaskada-ai/kaskada/clients/cli/utils"
	"github.com/spf13/cobra"

	apiv1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
)

// getCmd represents the materialization get command
var getCmd = &cobra.Command{
	Use:   "get",
	Short: "Gets a materialization.",
	Run: func(cmd *cobra.Command, args []string) {
		item, err := api.NewApiClient().Get(&apiv1alpha.Materialization{MaterializationName: materializationName})
		utils.LogAndQuitIfErrorExists(err)
		materialization, err := api.ProtoToMaterialization(item)
		utils.LogAndQuitIfErrorExists(err)
		yaml, err := utils.ProtoToYaml(materialization)
		utils.LogAndQuitIfErrorExists(err)
		fmt.Printf("\n%s\n", yaml)
	},
}

func init() {
	initMaterializationFlag(getCmd, "The materialization to get.")
}
