package view

import (
	"github.com/kaskada-ai/kaskada/clients/cli/api"
	"github.com/kaskada-ai/kaskada/clients/cli/utils"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	apiv1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
)

// createCmd represents the view create command
var createCmd = &cobra.Command{
	Use:   "create",
	Short: "Creates a view.",
	Run: func(cmd *cobra.Command, args []string) {
		newView := &apiv1alpha.View{
			ViewName:   viewName,
			Expression: expression,
		}

		utils.LogAndQuitIfErrorExists(api.NewApiClient().Create(newView))
		log.Info().Msg("Success!")
	},
}

var expression string

func init() {
	initViewFlag(createCmd, "The name of the view to create.")

	createCmd.Flags().StringVar(&expression, "expression", "", "The View's Fenl expression.")
	createCmd.MarkFlagRequired("expression")
}
