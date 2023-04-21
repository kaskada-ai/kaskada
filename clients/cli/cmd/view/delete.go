package view

import (
	"github.com/kaskada-ai/kaskada/clients/cli/api"
	"github.com/kaskada-ai/kaskada/clients/cli/utils"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	apiv1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
)

// deleteCmd represents the view delete command
var deleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "Deletes a view.",
	Run: func(cmd *cobra.Command, args []string) {
		utils.LogAndQuitIfErrorExists(api.NewApiClient().Delete(&apiv1alpha.View{ViewName: view}, force))
		log.Info().Msg("Success!")
	},
}

var force bool
func init() {
	initViewFlag(deleteCmd, "The view to delete.")

	deleteCmd.Flags().BoolVar(&force, "force", false, "Force delete the view. This could break existing materializations.")
}
