package utils

import (
	"fmt"
	"os"
	"strings"

	"github.com/goccy/go-yaml"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"

	apiv1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
)

func InitLogging() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
}

func FormatHelp(textWithNewLines string) string {
	return strings.ReplaceAll(strings.Trim(textWithNewLines, "\n"), "\n", " ")
}

func LogAndQuitIfErrorExists(err error) {
	if err != nil {
		log.Fatal().Err(err).Send()
	}
}

func SetupStandardResourceCmd(cmd *cobra.Command, method string, item string, additionalArgs ...string) {
	requiredArgs := make([]string, len(additionalArgs)+1, len(additionalArgs)+1)
	requiredArgs[0] = fmt.Sprintf("\"%s_name\"", item)
	for i, arg := range additionalArgs {
		requiredArgs[i+1] = fmt.Sprintf("\"%s\"", arg)
	}

	cmd.Use = fmt.Sprintf("%s %s", method, strings.Join(requiredArgs, " "))
	cmd.Short = fmt.Sprintf("%ss %s.", strings.Title(method), item)
	cmd.Args = func(cmd *cobra.Command, args []string) error {
		if len(args) < len(requiredArgs) {
			if len(requiredArgs) == 1 {
				return fmt.Errorf("\n %s is required\n", requiredArgs[0])
			} else {
				return fmt.Errorf("\n %s are required\n", strings.Join(requiredArgs, ", "))
			}
		}
		if len(args) > len(requiredArgs) {
			return fmt.Errorf("\n  Unexpected positional arguments after %s: %s\n", strings.Join(requiredArgs, " "), args[len(requiredArgs):])
		}
		return nil
	}
}

func SetupListResourceCmd(cmd *cobra.Command, item string) {
	cmd.Use = "list"
	cmd.Short = fmt.Sprintf("Lists %ss. Currently only the first page of %ss is returned", item, item)
	cmd.Args = cobra.NoArgs
}

func PrintSuccessf(format string, a ...any) {
	fmt.Printf("\n%s\n", fmt.Sprintf(format, a...))
}

func PrintDeleteSuccess(item string, name string) {
	PrintSuccessf("Successfully deleted %s: \"%s\"\n", item, name)
}

func PrintProtoMessage(proto protoreflect.ProtoMessage) {
	yaml, err := ProtoToYaml(proto)
	LogAndQuitIfErrorExists(err)
	PrintSuccessf("%s", yaml)
}

// ProtoToYaml converts a protobuf message to a yaml byte slice
func ProtoToYaml(proto protoreflect.ProtoMessage) ([]byte, error) {
	// Convert to a json first to drop all the null and empty fields in an unordered map
	jsonSpec, err := protojson.Marshal(proto)
	if err != nil {
		log.Debug().Err(err).Msg("issue converting spec to json")
		return nil, err
	}
	var v interface{}
	if err := yaml.UnmarshalWithOptions(jsonSpec, &v, yaml.UseOrderedMap()); err != nil {
		log.Debug().Err(err).Msg("failed to unmarshal from json bytes")
		return nil, err
	}
	yamlSpec, err := yaml.MarshalWithOptions(v, yaml.UseLiteralStyleIfMultiline(true))
	if err != nil {
		log.Debug().Err(err).Msg("issue converting spec to yaml")
		return nil, err
	}
	return yamlSpec, nil
}

// YamlToSpec converts a yaml byte slice to a Kaskada Spec
func YamlToSpec(yamlData []byte) (*apiv1alpha.Spec, error) {
	jsonSpec, err := yaml.YAMLToJSON(yamlData)
	if err != nil {
		log.Debug().Err(err).Msg("issue converting yaml to json")
		return nil, err
	}

	var spec apiv1alpha.Spec
	err = protojson.Unmarshal(jsonSpec, &spec)
	if err != nil {
		log.Debug().Err(err).Msg("issue converting json to spec")
		return nil, err
	}
	return &spec, nil
}
