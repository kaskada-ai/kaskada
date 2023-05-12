package cmd

import (
	"fmt"

	"github.com/kaskada-ai/kaskada/clients/cli/api"
	"github.com/kaskada-ai/kaskada/clients/cli/utils"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	apiv1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
)

// LoadCmd represents the load command
var LoadCmd = &cobra.Command{
	Deprecated: "use `table load` instead",
	Use:        "load",
	Short:      "Loads data into a table.",
	Run: func(cmd *cobra.Command, args []string) {
		log.Debug().Msg("starting load")

		var fileType apiv1alpha.FileType
		switch fileTypeIn {
		case "parquet", "":
			fileType = apiv1alpha.FileType_FILE_TYPE_PARQUET
		case "csv":
			fileType = apiv1alpha.FileType_FILE_TYPE_CSV
		default:
			utils.LogAndQuitIfErrorExists(fmt.Errorf("unrecognized file type - must be one of 'parquet', 'csv'"))
		}

		if len(filePaths) == 0 {
			utils.LogAndQuitIfErrorExists(fmt.Errorf("at least one `file` flag must be set"))
		}

		apiClient := api.NewApiClient()
		for _, file := range filePaths {
			err := apiClient.LoadFile(tableName, &apiv1alpha.FileInput{FileType: fileType, Uri: file})
			utils.LogAndQuitIfErrorExists(err)
		}
		log.Debug().Msg("Success!")
	},
}

var (
	fileTypeIn, tableName string
	filePaths             []string
)

func init() {
	LoadCmd.Flags().StringVar(&fileTypeIn, "file-type", "parquet", "(Optional) The type of file to load.  Either 'parquet' or 'csv'.")
	LoadCmd.Flags().StringVar(&tableName, "table", "", "The table to load data into.")
	LoadCmd.Flags().StringSliceVar(&filePaths, "file-path", []string{}, "The path of the file to load on the Kaskada instance.")
}
