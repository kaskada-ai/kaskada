package table

import (
	"github.com/kaskada-ai/kaskada/clients/cli/api"
	"github.com/kaskada-ai/kaskada/clients/cli/utils"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	apiv1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
)

// deleteCmd represents the table delete command
var deleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "Deletes a table.",
	Run: func(cmd *cobra.Command, args []string) {
		utils.LogAndQuitIfErrorExists(api.NewApiClient().Delete(&apiv1alpha.Table{TableName: tableName}, force))
		log.Info().Msg("Success!")
	},
}

var force bool
func init() {
	initTableFlag(deleteCmd, "The table to delete.")

	deleteCmd.Flags().BoolVar(&force, "force", false, "Force delete the table. This could break existing views and materializations.")
}
