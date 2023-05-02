package view

import (
	"fmt"

	"github.com/kaskada-ai/kaskada/clients/cli/api"
	"github.com/kaskada-ai/kaskada/clients/cli/utils"
	"github.com/spf13/cobra"

	apiv1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
)

// getCmd represents the view get command
var getCmd = &cobra.Command{
	Use:   "get",
	Short: "Gets a view.",
	Run: func(cmd *cobra.Command, args []string) {
		item, err := api.NewApiClient().Get(&apiv1alpha.View{ViewName: viewName})
		utils.LogAndQuitIfErrorExists(err)
		view, err := api.ProtoToView(item)
		utils.LogAndQuitIfErrorExists(err)
		yaml, err := utils.ProtoToYaml(view)
		utils.LogAndQuitIfErrorExists(err)
		fmt.Printf("\n%s\n", yaml)
	},
}

func init() {
	initViewFlag(getCmd, "The view to get.")
}
