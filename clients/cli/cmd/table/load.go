package table

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/kaskada-ai/kaskada/clients/cli/api"
	"github.com/kaskada-ai/kaskada/clients/cli/utils"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	apiv1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
)

// loadCmd represents the table load command
var loadCmd = &cobra.Command{
	Run: func(cmd *cobra.Command, args []string) {
		log.Debug().Msg("starting load")

		tableName := args[0]
		fileURI := args[1]

		if loadFileType == "" {
			loadFileType = strings.TrimLeft(filepath.Ext(fileURI), ".")
		}

		var fileType apiv1alpha.FileType
		switch loadFileType {
		case "parquet", "":
			fileType = apiv1alpha.FileType_FILE_TYPE_PARQUET
		case "csv":
			fileType = apiv1alpha.FileType_FILE_TYPE_CSV
		default:
			utils.LogAndQuitIfErrorExists(fmt.Errorf("unrecognized file type: %s - must be one of 'parquet', 'csv'", loadFileType))
		}

		apiClient := api.NewApiClient()
		err := apiClient.LoadFile(tableName, &apiv1alpha.FileInput{FileType: fileType, Uri: fileURI})
		utils.LogAndQuitIfErrorExists(err)
		utils.PrintSuccessf("Successfully loaded \"%s\" into \"%s\" table\n", filepath.Base(fileURI), tableName)
	},
}

var loadFileType string

func init() {
	utils.SetupStandardResourceCmd(loadCmd, "load", "table", "file_uri")
	loadCmd.Short = "Loads a file URI into a table. The file must be accessible from the Kaskada service."

	loadCmd.Flags().StringVarP(&loadFileType, "type", "t", "", "(Optional) The type of file to load.  Either 'parquet' or 'csv'.  Defaults to the file extension or 'parquet'.")
}
