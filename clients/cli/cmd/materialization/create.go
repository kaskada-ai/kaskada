package materialization

import (
	"github.com/kaskada-ai/kaskada/clients/cli/api"
	"github.com/kaskada-ai/kaskada/clients/cli/utils"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	apiv1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
)

// createCmd represents the materialization create command
var createCmd = &cobra.Command{
	Run: func(cmd *cobra.Command, args []string) {
		if objectStoreKind != "parquet" && objectStoreKind != "csv" {
			log.Fatal().Msg("file-kind must be one of: parquet, or csv")
		}

		newMaterialization := &apiv1alpha.Materialization{
			MaterializationName: args[0],
			Expression:          args[1],
		}

		if objectStoreDestination != "" {
			fileType := apiv1alpha.FileType_FILE_TYPE_PARQUET
			if objectStoreKind == "csv" {
				fileType = apiv1alpha.FileType_FILE_TYPE_CSV
			}

			newMaterialization.Destination = &apiv1alpha.Destination{
				Destination: &apiv1alpha.Destination_ObjectStore{
					ObjectStore: &apiv1alpha.ObjectStoreDestination{
						OutputPrefixUri: objectStoreDestination,
						FileType:        fileType,
					},
				},
			}
		}
		newItem, err := api.NewApiClient().Create(newMaterialization)
		utils.LogAndQuitIfErrorExists(err)
		printMaterialization(newItem)
	},
}

var objectStoreDestination string
var objectStoreKind string

func init() {
	utils.SetupStandardResourceCmd(createCmd, "create", materialization, "expression")

	createCmd.Flags().StringVarP(&objectStoreDestination, "path-uri", "u", "", "The path uri of where to push output to. Examples: s3://my-bucket/path/to/results/, file:///local/path/to/results/, etc. The kaskada service must have access to the destination.")
	createCmd.Flags().StringVarP(&objectStoreKind, "file-kind", "k", "csv", "The kind of file to output. Should be one of: csv, or parquet. Defaults to csv.")

	createCmd.MarkFlagRequired("path-uri")
}
