package table

import (
	"github.com/kaskada-ai/kaskada/clients/cli/api"
	"github.com/kaskada-ai/kaskada/clients/cli/utils"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/wrapperspb"

	apiv1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
)

var entityKeyHelpText = "The name of the entity key column within the table. The entity key identifies an entity associated with each row."
var subsortHelpText = "The name of the subsort column within the table. Subsort columns provide a global order across rows in a table sharing the same time. Subsort columns must be globally unique per row within a given time. It is recommended that the subsort column be populated with 64-bit random integers. If no subsort column is provided, the system will generate the subsort column as a random set of contiguous unsigned integers."
var timeHelpText = "The name of the time column within the table. Parquet files loaded into the table must include a column with the given name. The type of the column must be a 64-bit nanosecond timestamp. Note that legacy (Parquet v1) 96-bit timestamp are NOT SUPPORTED."

// createCmd represents the table create command
var createCmd = &cobra.Command{
	Run: func(cmd *cobra.Command, args []string) {
		newTable := &apiv1alpha.Table{
			TableName:           args[0],
			TimeColumnName:      timeColumnName,
			EntityKeyColumnName: entityKeyColumnName,
			GroupingId:          groupingId,
		}
		if subsortColumnName != "" {
			newTable.SubsortColumnName = &wrapperspb.StringValue{Value: subsortColumnName}
		}

		newItem, err := api.NewApiClient().Create(newTable)
		utils.LogAndQuitIfErrorExists(err)
		printTable(newItem)
	},
}

var timeColumnName string
var entityKeyColumnName string
var subsortColumnName string
var groupingId string

func init() {
	utils.SetupStandardResourceCmd(createCmd, "create", "table")

	createCmd.Flags().StringVarP(&timeColumnName, "timeColumn", "t", "", timeHelpText)
	createCmd.Flags().StringVarP(&entityKeyColumnName, "entityKeyColumn", "e", "", entityKeyHelpText)
	createCmd.Flags().StringVarP(&subsortColumnName, "subsortColumn", "s", "", subsortHelpText)
	createCmd.Flags().StringVarP(&groupingId, "groupingId", "g", "", "Optional field to enforce joins between multiple tables.")

	createCmd.MarkFlagRequired("timeColumn")
	createCmd.MarkFlagRequired("entityKeyColumn")
}
