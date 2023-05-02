package table

import (
	"fmt"

	"github.com/kaskada-ai/kaskada/clients/cli/api"
	"github.com/kaskada-ai/kaskada/clients/cli/utils"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	apiv1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
)

// loadCmd represents the load command
var loadCmd = &cobra.Command{
	Use:   "load",
	Short: "Loads data into a table.",
	Run: func(cmd *cobra.Command, args []string) {
		log.Info().Msg("starting load")

		var fileType apiv1alpha.FileType
		switch loadFileType {
		case "parquet", "":
			fileType = apiv1alpha.FileType_FILE_TYPE_PARQUET
		case "csv":
			fileType = apiv1alpha.FileType_FILE_TYPE_CSV
		default:
			utils.LogAndQuitIfErrorExists(fmt.Errorf("unrecognized file type - must be one of 'parquet', 'csv'"))
		}

		apiClient := api.NewApiClient()
		for _, file := range files {
			err := apiClient.LoadFile(tableName, &apiv1alpha.FileInput{FileType: fileType, Uri: file})
			utils.LogAndQuitIfErrorExists(err)
		}
		log.Info().Msg("Success!")
	},
}

var loadFileType string
var files []string

func init() {
	initTableFlag(loadCmd, "The table to load the file into.")
	loadCmd.Flags().StringVarP(&loadFileType, "kind", "k", "parquet", "(Optional) The kind of file to load.  Either 'parquet' or 'csv'.")
	loadCmd.Flags().StringArrayVarP(&files, "file", "f", []string{}, "The path of the file to load on the Kaskada instance.")
	loadCmd.MarkFlagRequired("file")
}
