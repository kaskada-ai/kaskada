package cmd

import (
	"fmt"

	"github.com/kaskada-ai/kaskada/clients/cli/api"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	apiv1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
)

// loadCmd represents the query command
var loadCmd = &cobra.Command{
	Use:   "load",
	Short: "A set of commands for loading data into kaskada",
	Run: func(cmd *cobra.Command, args []string) {
		log.Info().Msg("starting load")

		var fileType apiv1alpha.FileType
		switch viper.GetString("file-type") {
		case "parquet", "":
			fileType = apiv1alpha.FileType_FILE_TYPE_PARQUET
		case "csv":
			fileType = apiv1alpha.FileType_FILE_TYPE_CSV
		default:
			logAndQuitIfErrorExists(fmt.Errorf("unrecognized file type - must be one of 'parquet', 'csv'"))
		}
		table := viper.GetString("table")
		files := viper.GetStringSlice("file-path")

		if len(files) == 0 {
			logAndQuitIfErrorExists(fmt.Errorf("at least one `file` flag must be set"))
		}

		apiClient := api.NewApiClient()
		for _, file := range files {
			err := apiClient.LoadFile(table, &apiv1alpha.FileInput{FileType: fileType, Uri: file})
			logAndQuitIfErrorExists(err)
		}
		log.Info().Msg("Success!")
	},
}

func init() {
	rootCmd.AddCommand(loadCmd)

	loadCmd.Flags().String("file-type", "parquet", "(Optional) The type of file to load.  Either 'parquet' or 'csv'.")
	loadCmd.Flags().String("table", "", "The table to load data into.")
	loadCmd.Flags().String("file-path", "", "The path of the file to load on the Kaskada instance.")

	viper.BindPFlag("file-type", loadCmd.Flags().Lookup("file-type"))
	viper.BindPFlag("table", loadCmd.Flags().Lookup("table"))
	viper.BindPFlag("file-path", loadCmd.Flags().Lookup("file-path"))
}
