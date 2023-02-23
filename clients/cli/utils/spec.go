package utils

import (
	"github.com/goccy/go-yaml"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"

	apiv1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
)

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
