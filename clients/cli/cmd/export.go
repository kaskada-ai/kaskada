package cmd

import (
	"fmt"
	"os"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

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
		log.Info().Msg("starting export")

		var (
			err              error
			materializations []*apiv1alpha.Materialization
			tables           []*apiv1alpha.Table
			views            []*apiv1alpha.View
		)

		apiClient := api.NewApiClient()

		getAll := viper.GetBool("all")
		materializationNames := viper.GetStringSlice("materialization")
		tableNames := viper.GetStringSlice("table")
		viewNames := viper.GetStringSlice("view")

		if !getAll && len(materializationNames)+len(tableNames)+len(viewNames) == 0 {
			logAndQuitIfErrorExists(fmt.Errorf("either `all`, `materialization`, `table`, or `view` flag needs to be set at least once"))
		}

		if getAll {
			materializations, err = exportMaterializations(apiClient)
			logAndQuitIfErrorExists(err)
			tables, err = exportTables(apiClient)
			logAndQuitIfErrorExists(err)
			views, err = exportViews(apiClient)
			logAndQuitIfErrorExists(err)
		} else {
			for _, materializationName := range materializationNames {
				materialization, err := exportMaterialization(apiClient, materializationName)
				logAndQuitIfErrorExists(err)
				materializations = append(materializations, materialization)
			}
			for _, tableName := range tableNames {
				table, err := exportTable(apiClient, tableName)
				logAndQuitIfErrorExists(err)
				tables = append(tables, table)
			}
			for _, viewName := range viewNames {
				view, err := exportView(apiClient, viewName)
				logAndQuitIfErrorExists(err)
				views = append(views, view)
			}
		}

		spec := &apiv1alpha.Spec{
			Tables:           tables,
			Views:            views,
			Materializations: materializations,
		}

		yamlData, err := utils.ProtoToYaml(spec)
		logAndQuitIfErrorExists(err)

		filePath := viper.GetString("file-path")
		if filePath == "" {
			fmt.Printf("%s\n", yamlData)
		} else {
			logAndQuitIfErrorExists(os.WriteFile(filePath, yamlData, 0666))
		}
		log.Info().Msg("Success!")
	},
}

func exportMaterializations(apiClient api.ApiClient) ([]*apiv1alpha.Materialization, error) {
	protos, err := apiClient.List(&apiv1alpha.ListMaterializationsRequest{})
	if err != nil {
		return nil, err
	}
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
	protos, err := apiClient.List(&apiv1alpha.ListTablesRequest{})
	if err != nil {
		return nil, err
	}
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
	protos, err := apiClient.List(&apiv1alpha.ListViewsRequest{})
	if err != nil {
		return nil, err
	}
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
	return api.ProtoToMaterialization(proto)
}

func exportTable(apiClient api.ApiClient, tableName string) (*apiv1alpha.Table, error) {
	proto, err := apiClient.Get(&apiv1alpha.Table{
		TableName: tableName},
	)
	if err != nil {
		return nil, err
	}
	return api.ProtoToTable(proto)
}

func exportView(apiClient api.ApiClient, viewName string) (*apiv1alpha.View, error) {
	proto, err := apiClient.Get(&apiv1alpha.View{
		ViewName: viewName},
	)
	if err != nil {
		return nil, err
	}
	return api.ProtoToView(proto)
}

func init() {
	syncCmd.AddCommand(exportCmd)

	exportCmd.Flags().Bool("all", false, "set this flag to export all tables, views, and materializations in the system")
	exportCmd.Flags().StringP("file-path", "f", "", "specify a file path to export the resources to, instead of exporting to std-out")
	exportCmd.Flags().StringArrayP("materialization", "m", []string{}, "the name of the `materialization` resource to export.  This flag can be set multiple times to specify multiple materialization.")
	exportCmd.Flags().StringArrayP("table", "t", []string{}, "the name of the `table` resource to export.  This flag can be set multiple times to specify multiple tables.")
	exportCmd.Flags().StringArrayP("view", "v", []string{}, "the name of the `view` resource to export.  This flag can be set multiple times to specify multiple views.")

	viper.BindPFlag("all", exportCmd.Flags().Lookup("all"))
	viper.BindPFlag("file-path", exportCmd.Flags().Lookup("file-path"))
	viper.BindPFlag("materialization", exportCmd.Flags().Lookup("materialization"))
	viper.BindPFlag("table", exportCmd.Flags().Lookup("table"))
	viper.BindPFlag("view", exportCmd.Flags().Lookup("view"))
}
