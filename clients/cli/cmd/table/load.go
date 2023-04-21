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

var loadFileType string
var files []string
func init() {
	loadCmd.Flags().StringVarP(&loadFileType, "file-type", "k", "parquet", "(Optional) The type of file to load.  Either 'parquet' or 'csv'.")
	loadCmd.Flags().StringArrayVarP(&files, "file-path", "f", []string{}, "The path of the file to load on the Kaskada instance.")
}
