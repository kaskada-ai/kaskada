package utils

import (
	"github.com/goccy/go-yaml"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"

	apiv1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
)

// ProtoToYaml converts a protobuf message to a yaml byte slice
func ProtoToYaml(proto protoreflect.ProtoMessage) ([]byte, error) {
	// Convert to a json first to drop all the null and empty fields in an unordered map
	jsonSpec, err := protojson.Marshal(proto)
	if err != nil {
		return nil, errors.Wrap(err, "converting spec to json")
	}
	var v interface{}
	if err := yaml.UnmarshalWithOptions(jsonSpec, &v, yaml.UseOrderedMap()); err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal from json bytes")
	}
	yamlSpec, err := yaml.MarshalWithOptions(v, yaml.UseLiteralStyleIfMultiline(true))
	if err != nil {
		return nil, errors.Wrap(err, "converting spec to yaml")
	}
	return yamlSpec, nil
}

// YamlToSpec converts a yaml byte slice to a Kaskada Spec
func YamlToSpec(yamlData []byte) (*apiv1alpha.Spec, error) {
	jsonSpec, err := yaml.YAMLToJSON(yamlData)
	if err != nil {
		return nil, errors.Wrap(err, "converting yaml to json")
	}

	var spec apiv1alpha.Spec
	err = protojson.Unmarshal(jsonSpec, &spec)
	if err != nil {
		return nil, errors.Wrap(err, "converting json to spec")
	}
	return &spec, nil
}
