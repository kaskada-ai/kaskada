package sync

import (
	"fmt"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/kaskada-ai/kaskada/clients/cli/api"
	"github.com/kaskada-ai/kaskada/clients/cli/utils"
)

// planCmd represents the plan command
var planCmd = &cobra.Command{
	Use:   "plan",
	Short: "diffs the current state of the system against the configuration in the provided spec file(s)",
	Run: func(cmd *cobra.Command, args []string) {
		log.Info().Msg("starting plan")

		files := viper.GetStringSlice("plan_file")

		if len(files) == 0 {
			utils.LogAndQuitIfErrorExists(fmt.Errorf("at least one `file` flag must be set"))
		}

		apiClient := api.NewApiClient()
		_, err := utils.Plan(apiClient, files)
		utils.LogAndQuitIfErrorExists(err)
		log.Info().Msg("Success!")
	},
}

func init() {
	planCmd.Flags().StringArrayP("file", "f", []string{}, "specify one or more file-paths of yaml spec files")
	viper.BindPFlag("plan_file", planCmd.Flags().Lookup("file"))
}
