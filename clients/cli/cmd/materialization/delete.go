package materialization

import (
	"github.com/kaskada-ai/kaskada/clients/cli/api"
	"github.com/kaskada-ai/kaskada/clients/cli/utils"
	"github.com/spf13/cobra"

	apiv1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
)

// deleteCmd represents the materialization delete command
var deleteCmd = &cobra.Command{
	Run: func(cmd *cobra.Command, args []string) {
		utils.LogAndQuitIfErrorExists(api.NewApiClient().Delete(&apiv1alpha.Materialization{MaterializationName: args[0]}, true))
		utils.PrintDeleteSuccess(materialization, args[0])
	},
}

func init() {
	utils.SetupStandardResourceCmd(deleteCmd, "delete", materialization)
}
