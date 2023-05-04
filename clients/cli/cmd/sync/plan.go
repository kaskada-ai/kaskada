package sync

import (
	"fmt"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/kaskada-ai/kaskada/clients/cli/api"
	"github.com/kaskada-ai/kaskada/clients/cli/utils"
)

// planCmd represents the plan command
var planCmd = &cobra.Command{
	Use:   "plan",
	Short: "diffs the current state of the system against the configuration in the provided spec file(s)",
	Run: func(cmd *cobra.Command, args []string) {
		log.Debug().Msg("starting plan")

		if len(planFiles) == 0 {
			utils.LogAndQuitIfErrorExists(fmt.Errorf("at least one `file` flag must be set"))
		}

		apiClient := api.NewApiClient()
		_, err := plan(apiClient, planFiles)
		utils.LogAndQuitIfErrorExists(err)
		log.Debug().Msg("Success!")
	},
}

var planFiles []string

func init() {
	planCmd.Flags().StringArrayVarP(&planFiles, "file", "f", []string{}, "specify one or more file-paths of yaml spec files")
}
