package view

import (
	"github.com/kaskada-ai/kaskada/clients/cli/api"
	"github.com/kaskada-ai/kaskada/clients/cli/utils"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/reflect/protoreflect"

	apiv1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
)

const view = "view"

// ViewCmd represents the view command
var ViewCmd = &cobra.Command{
	Use:   view,
	Short: "A set of commands for interacting with kaskada views",
	/*
		Long: `A longer description that spans multiple lines and likely contains examples
		and usage of using your command. For example:

		Cobra is a CLI library for Go that empowers applications.
		This application is a tool to generate the needed files
		to quickly create a Cobra application.`,
	*/
}

func init() {
	ViewCmd.AddCommand(createCmd)
	ViewCmd.AddCommand(deleteCmd)
	ViewCmd.AddCommand(getCmd)
	ViewCmd.AddCommand(listCmd)
}

func getViewFromItem(item protoreflect.ProtoMessage) *apiv1alpha.View {
	view, err := api.ProtoToView(item)
	utils.LogAndQuitIfErrorExists(err)
	return view
}

func printView(item protoreflect.ProtoMessage) {
	view := getViewFromItem(item)
	yaml, err := utils.ProtoToYaml(view)
	utils.LogAndQuitIfErrorExists(err)
	utils.PrintSuccessf("%s", yaml)
}
