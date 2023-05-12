package view

import (
	"github.com/kaskada-ai/kaskada/clients/cli/api"
	"github.com/kaskada-ai/kaskada/clients/cli/utils"
	"github.com/spf13/cobra"

	apiv1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
)

// getCmd represents the view get command
var getCmd = &cobra.Command{
	Run: func(cmd *cobra.Command, args []string) {
		item, err := api.NewApiClient().Get(&apiv1alpha.View{ViewName: args[0]})
		utils.LogAndQuitIfErrorExists(err)
		printView(item)
	},
}

func init() {
	utils.SetupStandardResourceCmd(getCmd, "get", view)
}
