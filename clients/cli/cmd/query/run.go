package query

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/kaskada-ai/kaskada/clients/cli/api"
	"github.com/kaskada-ai/kaskada/clients/cli/utils"
	apiv1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	// preparev1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/prepare/v1alpha"
)

func init() {
	runCmd.Flags().String("response-as", "parquet", "(Optional) How to encode the results.  Either 'parquet' or 'csv'.")
	runCmd.Flags().String("data-token", "", "(Optional) A token to run queries against. Enables repeatable queries.")
	runCmd.Flags().String("result-behavior", "all-results", "(Optional) Determines how results are returned.  Either 'all-results' or 'final-results'.")
	runCmd.Flags().Int64("preview-rows", 0, "(Optional) Produces a preview of the results with at least this many rows.")
	runCmd.Flags().Bool("dry-run", false, "(Optional) If this is `true`, then the query is validated and if there are no errors, the resultant schema is returned. No actual computation of results is performed.")
	runCmd.Flags().Bool("experimental-features", false, "(Optional) If this is `true`, then experimental features are allowed.  Data returned when using this flag is not guaranteed to be correct.")
	runCmd.Flags().Int64("changed-since-time", 0, "(Optional) Unix timestamp bound (inclusive) after which results will be output. If 'response-behavior' is 'all-results', this will include rows for changes (events and ticks) after this time (inclusive). If it is 'final-results', this will include a final result for any entity that would be included in the changed results.")
	runCmd.Flags().Bool("stdout", false, "(Optional) If this is `true`, output results are sent to STDOUT")

	viper.BindPFlag("response_as", runCmd.Flags().Lookup("response-as"))
	viper.BindPFlag("data_token", runCmd.Flags().Lookup("data-token"))
	viper.BindPFlag("result_behavior", runCmd.Flags().Lookup("result-behavior"))
	viper.BindPFlag("preview_rows", runCmd.Flags().Lookup("preview-rows"))
	viper.BindPFlag("dry_run", runCmd.Flags().Lookup("dry-run"))
	viper.BindPFlag("experimental_features", runCmd.Flags().Lookup("experimental-features"))
	viper.BindPFlag("changed_since_time", runCmd.Flags().Lookup("changed-since-time"))
	viper.BindPFlag("stdout", runCmd.Flags().Lookup("stdout"))
}

// runCmd represents the query run command
var runCmd = &cobra.Command{
	Args:  cobra.MaximumNArgs(1),
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

		destination := &apiv1alpha.Destination{}

		responseAs := viper.GetString("response_as")
		switch responseAs {
		case "csv":
			destination.Destination = &apiv1alpha.Destination_ObjectStore{
				ObjectStore: &apiv1alpha.ObjectStoreDestination{
					FileType: apiv1alpha.FileType_FILE_TYPE_CSV,
				},
			}
		case "parquet":
			destination.Destination = &apiv1alpha.Destination_ObjectStore{
				ObjectStore: &apiv1alpha.ObjectStoreDestination{
					FileType: apiv1alpha.FileType_FILE_TYPE_PARQUET,
				},
			}
		default:
			utils.LogAndQuitIfErrorExists(fmt.Errorf("'response-as' must be 'csv' or 'parquet'"))
		}

		var expression string
		if len(args) == 0 {
			stdin, err := io.ReadAll(os.Stdin)
			utils.LogAndQuitIfErrorExists(err)
			expression = string(stdin)
		} else {
			expression = args[0]
		}
		queryReq := &apiv1alpha.CreateQueryRequest{
			Query: &apiv1alpha.Query{
				Expression:  expression,
				Destination: destination,
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
			utils.LogAndQuitIfErrorExists(fmt.Errorf("'result-behavior' must be 'all-results' or 'final-results'"))
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

		utils.LogAndQuitIfErrorExists(err)

		if viper.GetBool("stdout") && resp.Destination != nil {

			switch outputTo := resp.Destination.Destination.(type) {
			case *apiv1alpha.Destination_ObjectStore:
				for _, path := range outputTo.ObjectStore.OutputPaths.Paths {
					f, err := os.Open(strings.TrimPrefix(path, `file://`))
					utils.LogAndQuitIfErrorExists(err)

					_, err = io.Copy(os.Stdout, f)
					utils.LogAndQuitIfErrorExists(err)
				}
			}
		} else {
			jsonBytes, err := protojson.Marshal(resp)

			utils.LogAndQuitIfErrorExists(err)

			var out bytes.Buffer
			json.Indent(&out, jsonBytes, "", "\t")
			out.WriteString("\n")
			out.WriteTo(os.Stdout)
		}

		log.Info().Msg("Success!")
	},
}
