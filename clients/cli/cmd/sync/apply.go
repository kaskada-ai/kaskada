package sync

import (
	"fmt"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/kaskada-ai/kaskada/clients/cli/api"
	"github.com/kaskada-ai/kaskada/clients/cli/utils"
)

// applyCmd represents the apply command
var applyCmd = &cobra.Command{
	Use:   "apply",
	Short: "updates the state of the system to match the configuration in the provided spec file(s)",
	/*
		Long: `A longer description that spans multiple lines and likely contains examples
		and usage of using your command. For example:

		Cobra is a CLI library for Go that empowers applications.
		This application is a tool to generate the needed files
		to quickly create a Cobra application.`,
	*/
	Run: func(cmd *cobra.Command, args []string) {
		log.Info().Msg("starting apply")

		log.Debug().Interface("files", applyFiles).Send()
		if len(applyFiles) == 0 {
			utils.LogAndQuitIfErrorExists(fmt.Errorf("at least one `file` flag must be set"))
		}

		apiClient := api.NewApiClient()
		planResult, err := plan(apiClient, applyFiles)
		utils.LogAndQuitIfErrorExists(err)
		utils.LogAndQuitIfErrorExists(apply(apiClient, *planResult))
		utils.PrintSuccessf("Successfully applied %d changes", len(planResult.resourcesToCreate)+len(planResult.resourcesToSkip))
	},
}

var applyFiles []string

func init() {
	applyCmd.Flags().StringArrayVarP(&applyFiles, "file", "f", []string{}, "specify one or more file-paths of yaml spec files")
}
