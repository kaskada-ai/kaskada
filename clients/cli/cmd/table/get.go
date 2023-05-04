package table

import (
	"github.com/kaskada-ai/kaskada/clients/cli/api"
	"github.com/kaskada-ai/kaskada/clients/cli/utils"
	"github.com/spf13/cobra"

	apiv1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
)

// getCmd represents the table get command
var getCmd = &cobra.Command{
	Run: func(cmd *cobra.Command, args []string) {
		item, err := api.NewApiClient().Get(&apiv1alpha.Table{TableName: args[0]})
		utils.LogAndQuitIfErrorExists(err)
		printTable(item)
	},
}

func init() {
	utils.SetupStandardResourceCmd(getCmd, "get", "table")
}
