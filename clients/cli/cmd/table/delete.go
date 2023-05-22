package table

import (
	"github.com/kaskada-ai/kaskada/clients/cli/api"
	"github.com/kaskada-ai/kaskada/clients/cli/utils"
	"github.com/spf13/cobra"

	apiv1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
)

// deleteCmd represents the table delete command
var deleteCmd = &cobra.Command{
	Run: func(cmd *cobra.Command, args []string) {
		utils.LogAndQuitIfErrorExists(api.NewApiClient().Delete(&apiv1alpha.Table{TableName: args[0]}, force))
		utils.PrintDeleteSuccess("table", args[0])
	},
}

var force bool

func init() {
	utils.SetupStandardResourceCmd(deleteCmd, "delete", "table")

	deleteCmd.Flags().BoolVar(&force, "force", false, "Force delete the table. This could break existing views and materializations.")
}
