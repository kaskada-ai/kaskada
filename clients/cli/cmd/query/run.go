package query

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/kaskada-ai/kaskada/clients/cli/api"
	"github.com/kaskada-ai/kaskada/clients/cli/utils"
	apiv1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
)

var (
	responseAs, dataToken, resultBehavior string
	previewRows, changedSinceTime         int64
	dryRun, experimentalFeatures, stdOut  bool
)

func init() {
	runCmd.Flags().StringVarP(&responseAs, "response-as", "t", "csv", "(Optional) How to encode the results.  Either 'parquet' or 'csv' (default).")
	runCmd.Flags().StringVarP(&dataToken, "data-token", "d", "", "(Optional) A token to run queries against. Enables repeatable queries.")
	runCmd.Flags().StringVarP(&resultBehavior, "result-behavior", "r", "all-results", "(Optional) Determines how results are returned.  Either 'all-results' or 'final-results'.")
	runCmd.Flags().Int64VarP(&previewRows, "preview-rows", "p", 0, "(Optional) Produces a preview of the results with at least this many rows.")
	runCmd.Flags().BoolVar(&dryRun, "dry-run", false, "(Optional) If this is `true`, then the query is validated and if there are no errors, the resultant schema is returned. No actual computation of results is performed.")
	runCmd.Flags().BoolVar(&experimentalFeatures, "experimental-features", false, "(Optional) If this is `true`, then experimental features are allowed.  Data returned when using this flag is not guaranteed to be correct.")
	runCmd.Flags().Int64VarP(&changedSinceTime, "changed-since-time", "c", 0, "(Optional) Unix timestamp bound (inclusive) after which results will be output. If 'response-behavior' is 'all-results', this will include rows for changes (events and ticks) after this time (inclusive). If it is 'final-results', this will include a final result for any entity that would be included in the changed results.")
	runCmd.Flags().BoolVar(&stdOut, "stdout", false, "(Optional) If this is `true`, output results are sent to STDOUT")
}

// runCmd represents the query run command
var runCmd = &cobra.Command{
	Args:  cobra.MaximumNArgs(1),
	Use:   "run \"expression-text\"",
	Short: "executes an expression on kaskada",
	/*
		Long: `A longer description that spans multiple lines and likely contains examples
		and usage of using your command. For example:

		Cobra is a CLI library for Go that empowers applications.
		This application is a tool to generate the needed files
		to quickly create a Cobra application.`,
	*/
	Run: func(cmd *cobra.Command, args []string) {
		log.Debug().Msg("starting query run")

		destination := &apiv1alpha.Destination{}

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
			fmt.Printf("\nEnter the expression to run and then press CTRL+D to execute it, or CTRL+C to cancel:\n\n")
			stdin, err := io.ReadAll(os.Stdin)
			utils.LogAndQuitIfErrorExists(err)
			fmt.Printf("\n\nExecuting query...\n\n")
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
				DryRun:               dryRun,
				ExperimentalFeatures: experimentalFeatures,
				StreamMetrics:        false,
			},
		}

		if dataToken != "" {
			queryReq.Query.DataTokenId = &wrapperspb.StringValue{Value: dataToken}
		}

		switch resultBehavior {
		case "all-results":
			queryReq.Query.ResultBehavior = apiv1alpha.Query_RESULT_BEHAVIOR_ALL_RESULTS
		case "final-results":
			queryReq.Query.ResultBehavior = apiv1alpha.Query_RESULT_BEHAVIOR_FINAL_RESULTS
		default:
			utils.LogAndQuitIfErrorExists(fmt.Errorf("'result-behavior' must be 'all-results' or 'final-results'"))
		}

		if previewRows > 0 {
			queryReq.Query.Limits = &apiv1alpha.Query_Limits{
				PreviewRows: previewRows,
			}
		}

		if changedSinceTime > 0 {
			queryReq.Query.ChangedSinceTime = timestamppb.New(time.Unix(changedSinceTime, 0))
		}

		resp, err := api.NewApiClient().Query(queryReq)
		utils.LogAndQuitIfErrorExists(err)

		if stdOut && resp.Destination != nil {
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
			yaml, err := utils.ProtoToYaml(resp)
			utils.LogAndQuitIfErrorExists(err)
			utils.PrintSuccessf("%s", yaml)
		}

		log.Debug().Msg("Success!")
	},
}
