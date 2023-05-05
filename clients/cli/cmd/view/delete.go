package view

import (
	"github.com/kaskada-ai/kaskada/clients/cli/api"
	"github.com/kaskada-ai/kaskada/clients/cli/utils"
	"github.com/spf13/cobra"

	apiv1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
)

// deleteCmd represents the view delete command
var deleteCmd = &cobra.Command{
	Run: func(cmd *cobra.Command, args []string) {
		utils.LogAndQuitIfErrorExists(api.NewApiClient().Delete(&apiv1alpha.View{ViewName: args[0]}, force))
		utils.PrintDeleteSuccess(view, args[0])
	},
}

var force bool

func init() {
	utils.SetupStandardResourceCmd(deleteCmd, "delete", view)

	deleteCmd.Flags().BoolVar(&force, "force", false, "Force delete the view. This could break existing materializations.")
}
