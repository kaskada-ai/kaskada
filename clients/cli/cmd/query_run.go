package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/kaskada-ai/kaskada/clients/cli/api"
	apiv1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	// preparev1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/prepare/v1alpha"
)

func init() {
	queryCmd.AddCommand(queryRunCmd)

	queryRunCmd.Flags().String("response-as", "parquet", "(Optional) How to encode the results.  Either 'parquet' or 'csv'.")
	queryRunCmd.Flags().String("data-token", "", "(Optional) A token to run queries against. Enables repeatable queries.")
	queryRunCmd.Flags().String("result-behavior", "all-results", "(Optional) Determines how results are returned.  Either 'all-results' or 'final-results'.")
	queryRunCmd.Flags().Int64("preview-rows", 0, "(Optional) Produces a preview of the results with at least this many rows.")
	queryRunCmd.Flags().Bool("dry-run", false, "(Optional) If this is `true`, then the query is validated and if there are no errors, the resultant schema is returned. No actual computation of results is performed.")
	queryRunCmd.Flags().Bool("experimental-features", false, "(Optional) If this is `true`, then experimental features are allowed.  Data returned when using this flag is not guaranteed to be correct.")
	queryRunCmd.Flags().Int64("changed-since-time", 0, "(Optional) Unix timestamp bound (inclusive) after which results will be output. If 'response-behavior' is 'all-results', this will include rows for changes (events and ticks) after this time (inclusive). If it is 'final-results', this will include a final result for any entity that would be included in the changed results.")

	viper.BindPFlag("response_as", queryRunCmd.Flags().Lookup("response-as"))
	viper.BindPFlag("data_token", queryRunCmd.Flags().Lookup("data-token"))
	viper.BindPFlag("result_behavior", queryRunCmd.Flags().Lookup("result-behavior"))
	viper.BindPFlag("preview_rows", queryRunCmd.Flags().Lookup("preview-rows"))
	viper.BindPFlag("dry_run", queryRunCmd.Flags().Lookup("dry-run"))
	viper.BindPFlag("experimental_features", queryRunCmd.Flags().Lookup("experimental-features"))
	viper.BindPFlag("changed_since_time", queryRunCmd.Flags().Lookup("changed-since-time"))
}

// queryRunCmd represents the queryRun command
var queryRunCmd = &cobra.Command{
	Args:  cobra.ExactArgs(1),
	Use:   "run \"query-text\"",
	Short: "executes a query on kaskada",
	/*
		Long: `A longer description that spans multiple lines and likely contains examples
		and usage of using your command. For example:

		Cobra is a CLI library for Go that empowers applications.
		This application is a tool to generate the needed files
		to quickly create a Cobra application.`,
	*/
	Run: func(cmd *cobra.Command, args []string) {
		log.Info().Msg("starting query run")

		responseAsFiles := &apiv1alpha.AsFiles{}

		responseAs := viper.GetString("response_as")
		switch responseAs {
		case "csv":
			responseAsFiles.FileType = apiv1alpha.FileType_FILE_TYPE_CSV
		case "parquet":
			responseAsFiles.FileType = apiv1alpha.FileType_FILE_TYPE_PARQUET
		default:
			logAndQuitIfErrorExists(fmt.Errorf("'response-as' must be 'csv' or 'parquet'"))
		}

		queryReq := &apiv1alpha.CreateQueryRequest{
			Query: &apiv1alpha.Query{
				Expression: args[0],
				ResponseAs: &apiv1alpha.Query_AsFiles{
					AsFiles: responseAsFiles,
				},
			},
			QueryOptions: &apiv1alpha.QueryOptions{
				DryRun:               viper.GetBool("dry_run"),
				ExperimentalFeatures: viper.GetBool("experimental_features"),
				StreamMetrics:        false,
			},
		}

		dataToken := viper.GetString("data_token")
		if dataToken != "" {
			queryReq.Query.DataTokenId = &wrapperspb.StringValue{Value: dataToken}
		}

		resultBehavior := viper.GetString("result_behavior")
		switch resultBehavior {
		case "all-results":
			queryReq.Query.ResultBehavior = apiv1alpha.Query_RESULT_BEHAVIOR_ALL_RESULTS
		case "final-results":
			queryReq.Query.ResultBehavior = apiv1alpha.Query_RESULT_BEHAVIOR_FINAL_RESULTS
		default:
			logAndQuitIfErrorExists(fmt.Errorf("'result-behavior' must be 'all-results' or 'final-results'"))
		}

		previewRows := viper.GetInt64("preview_rows")
		if previewRows > 0 {
			queryReq.Query.Limits = &apiv1alpha.Query_Limits{
				PreviewRows: previewRows,
			}
		}

		changedSinceTime := viper.GetInt64("changed_since_time")
		if changedSinceTime > 0 {
			queryReq.Query.ChangedSinceTime = timestamppb.New(time.Unix(changedSinceTime, 0))
		}

		resp, err := api.NewApiClient().Query(queryReq)

		logAndQuitIfErrorExists(err)

		jsonBytes, err := protojson.Marshal(resp)

		logAndQuitIfErrorExists(err)

		var out bytes.Buffer
		json.Indent(&out, jsonBytes, "", "\t")
		out.WriteString("\n")
		out.WriteTo(os.Stdout)

		log.Info().Msg("Success!")
	},
}
