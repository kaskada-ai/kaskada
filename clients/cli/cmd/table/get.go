package table

import (
	"fmt"

	"github.com/kaskada-ai/kaskada/clients/cli/api"
	"github.com/kaskada-ai/kaskada/clients/cli/utils"
	"github.com/spf13/cobra"

	apiv1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
)

// getCmd represents the table get command
var getCmd = &cobra.Command{
	Use:   "get",
	Short: "Gets a table.",
	Run: func(cmd *cobra.Command, args []string) {
		item, err := api.NewApiClient().Get(&apiv1alpha.Table{TableName: tableName})
		utils.LogAndQuitIfErrorExists(err)
		table, err := api.ProtoToTable(item)
		utils.LogAndQuitIfErrorExists(err)
		switch table.Source.Source.(type) {
		case *apiv1alpha.Source_Kaskada:
			table.Source = nil
		}

		yaml, err := utils.ProtoToYaml(table)
		utils.LogAndQuitIfErrorExists(err)
		fmt.Printf("\n%s\n", yaml)
	},
}

func init() {
	initTableFlag(getCmd, "The table to get.")
}
