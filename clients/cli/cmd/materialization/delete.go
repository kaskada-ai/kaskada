package materialization

import (
	"github.com/kaskada-ai/kaskada/clients/cli/api"
	"github.com/kaskada-ai/kaskada/clients/cli/utils"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	apiv1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
)

// deleteCmd represents the materialization delete command
var deleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "Deletes a materialization.",
	Run: func(cmd *cobra.Command, args []string) {
		utils.LogAndQuitIfErrorExists(api.NewApiClient().Delete(&apiv1alpha.Materialization{MaterializationName: materializationName}, true))
		log.Info().Msg("Success!")
	},
}

func init() {
	initMaterializationFlag(deleteCmd, "The materialization to delete.")
}
