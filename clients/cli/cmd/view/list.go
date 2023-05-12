package view

import (
	"strings"

	"github.com/kaskada-ai/kaskada/clients/cli/api"
	"github.com/kaskada-ai/kaskada/clients/cli/utils"
	"github.com/spf13/cobra"

	apiv1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
)

// listCmd represents the view list command
var listCmd = &cobra.Command{
	Run: func(cmd *cobra.Command, args []string) {
		items, err := api.NewApiClient().List(&apiv1alpha.View{}, search, pageSize, "")
		utils.LogAndQuitIfErrorExists(err)

		if printAllDetails {
			for _, item := range items {
				printView(item)
			}
		} else {
			names := make([]string, len(items))
			for i, item := range items {
				names[i] = getViewFromItem(item).ViewName
			}
			utils.PrintSuccessf("%s\n", strings.Join(names, "\n"))
		}

	},
}

var search string
var pageSize int32
var printAllDetails bool

func init() {
	utils.SetupListResourceCmd(listCmd, "view")

	listCmd.Flags().StringVarP(&search, "search", "s", "", "(Optional) Search string")
	listCmd.Flags().Int32VarP(&pageSize, "page-size", "p", 10, "(Optional) Page size")
	listCmd.Flags().BoolVarP(&printAllDetails, "all-details", "a", false, "(Optional) Print all details for each view")
}
