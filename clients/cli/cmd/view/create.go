package view

import (
	"github.com/kaskada-ai/kaskada/clients/cli/api"
	"github.com/kaskada-ai/kaskada/clients/cli/utils"
	"github.com/spf13/cobra"

	apiv1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
)

// createCmd represents the view create command
var createCmd = &cobra.Command{
	Run: func(cmd *cobra.Command, args []string) {
		newView := &apiv1alpha.View{
			ViewName:   args[0],
			Expression: args[1],
		}

		newItem, err := api.NewApiClient().Create(newView)
		utils.LogAndQuitIfErrorExists(err)
		printView(newItem)
	},
}

func init() {
	utils.SetupStandardResourceCmd(createCmd, "create", view, "expression")
}
