package sync

import (
	"fmt"
	"os"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/kaskada-ai/kaskada/clients/cli/api"
	"github.com/kaskada-ai/kaskada/clients/cli/utils"
	apiv1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
)

// exportCmd represents the export command
var exportCmd = &cobra.Command{
	Use:   "export",
	Short: "takes a set of resources and returns a yaml file",
	Long: `Used to generate resources-as-code for kaskada tables,
	 	    views, and materializations.  The outputted yaml file
		    can be used with the 'plan' and 'apply' commands
		    to update your infrastructure.`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Debug().Msg("starting export")

		var (
			err              error
			materializations []*apiv1alpha.Materialization
			tables           []*apiv1alpha.Table
			views            []*apiv1alpha.View
		)

		apiClient := api.NewApiClient()

		if !getAll && len(materializationNames)+len(tableNames)+len(viewNames) == 0 {
			utils.LogAndQuitIfErrorExists(fmt.Errorf("either `all`, `materialization`, `table`, or `view` flag needs to be set at least once"))
		}

		if getAll {
			materializations, err = exportMaterializations(apiClient)
			utils.LogAndQuitIfErrorExists(err)
			tables, err = exportTables(apiClient)
			utils.LogAndQuitIfErrorExists(err)
			views, err = exportViews(apiClient)
			utils.LogAndQuitIfErrorExists(err)
		} else {
			for _, materializationName := range materializationNames {
				materialization, err := exportMaterialization(apiClient, materializationName)
				utils.LogAndQuitIfErrorExists(err)
				materializations = append(materializations, materialization)
			}
			for _, tableName := range tableNames {
				table, err := exportTable(apiClient, tableName)
				utils.LogAndQuitIfErrorExists(err)
				tables = append(tables, table)
			}
			for _, viewName := range viewNames {
				view, err := exportView(apiClient, viewName)
				utils.LogAndQuitIfErrorExists(err)
				views = append(views, view)
			}
		}

		spec := &apiv1alpha.Spec{
			Tables:           tables,
			Views:            views,
			Materializations: materializations,
		}

		yamlData, err := utils.ProtoToYaml(spec)
		utils.LogAndQuitIfErrorExists(err)

		if filePath == "" {
			fmt.Printf("%s\n", yamlData)
		} else {
			utils.LogAndQuitIfErrorExists(os.WriteFile(filePath, yamlData, 0666))
		}
		log.Debug().Msg("Success!")
	},
}

func exportMaterializations(apiClient api.ApiClient) ([]*apiv1alpha.Materialization, error) {
	protos, err := apiClient.List(&apiv1alpha.ListMaterializationsRequest{}, "", 100, "")
	if err != nil {
		return nil, err
	}
	api.ClearOutputOnlyFieldsList(protos)

	materializations := make([]*apiv1alpha.Materialization, 0)
	for _, p := range protos {
		m, err := api.ProtoToMaterialization(p)
		if err != nil {
			return nil, err
		}
		materializations = append(materializations, m)
	}
	return materializations, nil
}

func exportTables(apiClient api.ApiClient) ([]*apiv1alpha.Table, error) {
	protos, err := apiClient.List(&apiv1alpha.ListTablesRequest{}, "", 100, "")
	if err != nil {
		return nil, err
	}
	api.ClearOutputOnlyFieldsList(protos)

	tables := make([]*apiv1alpha.Table, 0)
	for _, p := range protos {
		t, err := api.ProtoToTable(p)
		if err != nil {
			return nil, err
		}
		tables = append(tables, t)
	}
	return tables, nil
}

func exportViews(apiClient api.ApiClient) ([]*apiv1alpha.View, error) {
	protos, err := apiClient.List(&apiv1alpha.ListViewsRequest{}, "", 100, "")
	if err != nil {
		return nil, err
	}
	api.ClearOutputOnlyFieldsList(protos)

	views := make([]*apiv1alpha.View, 0)
	for _, p := range protos {
		v, err := api.ProtoToView(p)
		if err != nil {
			return nil, err
		}
		views = append(views, v)
	}
	return views, nil
}

func exportMaterialization(apiClient api.ApiClient, materializationName string) (*apiv1alpha.Materialization, error) {
	proto, err := apiClient.Get(&apiv1alpha.Materialization{
		MaterializationName: materializationName},
	)
	if err != nil {
		return nil, err
	}
	return api.ProtoToMaterialization(api.ClearOutputOnlyFields(proto))
}

func exportTable(apiClient api.ApiClient, tableName string) (*apiv1alpha.Table, error) {
	proto, err := apiClient.Get(&apiv1alpha.Table{
		TableName: tableName},
	)
	if err != nil {
		return nil, err
	}
	return api.ProtoToTable(api.ClearOutputOnlyFields(proto))
}

func exportView(apiClient api.ApiClient, viewName string) (*apiv1alpha.View, error) {
	proto, err := apiClient.Get(&apiv1alpha.View{
		ViewName: viewName},
	)
	if err != nil {
		return nil, err
	}
	return api.ProtoToView(api.ClearOutputOnlyFields(proto))
}

var (
	getAll                                      bool
	filePath                                    string
	materializationNames, tableNames, viewNames []string
)

func init() {
	exportCmd.Flags().BoolVar(&getAll, "all", false, "set this flag to export all tables, views, and materializations in the system")
	exportCmd.Flags().StringVarP(&filePath, "file-path", "f", "", "specify a file path to export the resources to, instead of exporting to std-out")
	exportCmd.Flags().StringArrayVarP(&materializationNames, "materialization", "m", []string{}, "the name of the `materialization` resource to export.  This flag can be set multiple times to specify multiple materialization.")
	exportCmd.Flags().StringArrayVarP(&tableNames, "table", "t", []string{}, "the name of the `table` resource to export.  This flag can be set multiple times to specify multiple tables.")
	exportCmd.Flags().StringArrayVarP(&viewNames, "view", "v", []string{}, "the name of the `view` resource to export.  This flag can be set multiple times to specify multiple views.")
}
