package materialization

import (
	"fmt"

	"github.com/kaskada-ai/kaskada/clients/cli/api"
	"github.com/kaskada-ai/kaskada/clients/cli/config"
	"github.com/kaskada-ai/kaskada/clients/cli/utils"
	"github.com/spf13/cobra"

	apiv1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
)

const destinationTypeHelp = `
The destination for the materialization.
(one-of "object-store" or "pulsar")
`

// createCmd represents the materialization create command
var createCmd = &cobra.Command{
	RunE: func(cmd *cobra.Command, args []string) error {

		var destination *apiv1alpha.Destination
		switch destinationType {
		case "object-store":
			osd, err := objectStoreDestination.ToProto()
			if err != nil {
				return err
			}
			destination = &apiv1alpha.Destination{
				Destination: &apiv1alpha.Destination_ObjectStore{
					ObjectStore: osd,
				},
			}
		case "pulsar":
			pc, err := pulsarConfig.ToProto()
			if err != nil {
				return err
			}
			destination = &apiv1alpha.Destination{
				Destination: &apiv1alpha.Destination_Pulsar{
					Pulsar: &apiv1alpha.PulsarDestination{
						Config: pc,
					},
				},
			}
		default:
			return fmt.Errorf("destination-type must be one of: object-store or pulsar")
		}

		newMaterialization := &apiv1alpha.Materialization{
			MaterializationName: args[0],
			Expression:          args[1],
			Destination:         destination,
		}
		newItem, err := api.NewApiClient().Create(newMaterialization)
		utils.LogAndQuitIfErrorExists(err)
		printMaterialization(newItem)
		return nil
	},
}

var pulsarConfig config.PulsarConfig
var objectStoreDestination config.ObjectStoreDestination
var destinationType string

func init() {
	utils.SetupStandardResourceCmd(createCmd, "create", materialization, "expression")

	createCmd.Flags().StringVar(&destinationType, "destination-type", "object-store", utils.FormatHelp(destinationTypeHelp))

	pulsarConfig = config.NewPulsarConfig()
	pulsarConfig.AddFlagsToCommand(createCmd)

	objectStoreDestination = config.NewObjectStoreDestination()
	objectStoreDestination.AddFlagsToCommand(createCmd)
}
