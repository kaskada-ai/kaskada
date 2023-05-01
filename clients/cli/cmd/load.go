package cmd

import (
	"fmt"

	"github.com/kaskada-ai/kaskada/clients/cli/api"
	"github.com/kaskada-ai/kaskada/clients/cli/utils"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	apiv1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
)

// LoadCmd represents the load command
var LoadCmd = &cobra.Command{
	Use:   "load",
	Short: "(deprecated) Use `table load` instead. Loads data into a table.",
	Run: func(cmd *cobra.Command, args []string) {
		log.Info().Msg("starting load")

		var fileType apiv1alpha.FileType
		switch viper.GetString("file-type") {
		case "parquet", "":
			fileType = apiv1alpha.FileType_FILE_TYPE_PARQUET
		case "csv":
			fileType = apiv1alpha.FileType_FILE_TYPE_CSV
		default:
			utils.LogAndQuitIfErrorExists(fmt.Errorf("unrecognized file type - must be one of 'parquet', 'csv'"))
		}
		table := viper.GetString("table")
		files := viper.GetStringSlice("file-path")

		if len(files) == 0 {
			utils.LogAndQuitIfErrorExists(fmt.Errorf("at least one `file` flag must be set"))
		}

		apiClient := api.NewApiClient()
		for _, file := range files {
			err := apiClient.LoadFile(table, &apiv1alpha.FileInput{FileType: fileType, Uri: file})
			utils.LogAndQuitIfErrorExists(err)
		}
		log.Info().Msg("Success!")
	},
}

func init() {
	LoadCmd.Flags().String("file-type", "parquet", "(Optional) The type of file to load.  Either 'parquet' or 'csv'.")
	LoadCmd.Flags().String("table", "", "The table to load data into.")
	LoadCmd.Flags().String("file-path", "", "The path of the file to load on the Kaskada instance.")
}
