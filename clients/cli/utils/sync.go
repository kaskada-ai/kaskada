package utils

import (
	"os"
	"reflect"

	"github.com/kaskada-ai/kaskada/clients/cli/api"
	"github.com/rs/zerolog/log"
	"github.com/sergi/go-diff/diffmatchpatch"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type planResult struct {
	resourcesToCreate  []protoreflect.ProtoMessage
	resourcesToReplace []protoreflect.ProtoMessage
	resourcesToSkip    []protoreflect.ProtoMessage
}

func Plan(apiClient api.ApiClient, files []string) (*planResult, error) {
	// get combined spec from all the files
	materializations := map[string]interface{}{}
	tables := map[string]interface{}{}
	views := map[string]interface{}{}
	desiredResources := []protoreflect.ProtoMessage{}

	for _, file := range files {
		contents, err := os.ReadFile(file)
		if err != nil {
			return nil, err
		}
		spec, err := YamlToSpec(contents)
		if err != nil {
			return nil, err
		}
		for _, materialization := range spec.Materializations {
			if _, found := materializations[materialization.MaterializationName]; !found {
				materializations[materialization.MaterializationName] = nil
				desiredResources = append(desiredResources, materialization)
			} else {
				log.Warn().Str("name", materialization.MaterializationName).Str("file", file).Msg("skipping duplicate materialization")
			}
		}

		for _, table := range spec.Tables {
			if _, found := tables[table.TableName]; !found {
				tables[table.TableName] = nil
				desiredResources = append(desiredResources, table)
			} else {
				log.Warn().Str("name", table.TableName).Str("file", file).Msg("skipping duplicate table")
			}
		}

		for _, view := range spec.Views {
			if _, found := views[view.ViewName]; !found {
				views[view.ViewName] = nil
				desiredResources = append(desiredResources, view)
			} else {
				log.Warn().Str("name", view.ViewName).Str("file", file).Msg("skipping duplicate view")
			}
		}
	}

	result := planResult{
		resourcesToCreate:  []protoreflect.ProtoMessage{},
		resourcesToReplace: []protoreflect.ProtoMessage{},
		resourcesToSkip:    []protoreflect.ProtoMessage{},
	}

	// diff combined spec with reality
	for _, desired := range desiredResources {
		switch compareResource(apiClient, desired) {
		case Create:
			result.resourcesToCreate = append(result.resourcesToCreate, desired)
		case Replace:
			result.resourcesToReplace = append(result.resourcesToReplace, desired)
		default:
			result.resourcesToSkip = append(result.resourcesToSkip, desired)
		}
	}
	return &result, nil
}

func Apply(apiClient api.ApiClient, plan planResult) error {
	for _, resource := range plan.resourcesToCreate {
		subLogger := log.With().Str("kind", reflect.TypeOf(resource).String()).Str("name", api.GetName(resource)).Logger()
		if err := apiClient.Create(resource); err != nil {
			subLogger.Error().Err(err).Str("kind", reflect.TypeOf(resource).String()).Str("name", api.GetName(resource)).Msg("issue creating resource")
			return err
		}
		subLogger.Info().Msg("created resource with provided spec")
	}
	for _, resource := range plan.resourcesToReplace {
		subLogger := log.With().Str("kind", reflect.TypeOf(resource).String()).Str("name", api.GetName(resource)).Logger()
		if err := apiClient.Delete(resource, true); err != nil {
			subLogger.Error().Err(err).Msg("issue deleting resource before re-create")
			return err
		}
		if err := apiClient.Create(resource); err != nil {
			subLogger.Error().Err(err).Msg("issue re-creating resource after delete")
			return err
		}
		subLogger.Info().Msg("updated resource with provided spec")
	}
	return nil
}

type CompareResult int

const (
	Error   CompareResult = 0
	Skip    CompareResult = 1
	Create  CompareResult = 2
	Replace CompareResult = 3
)

func compareResource(apiClient api.ApiClient, desired protoreflect.ProtoMessage) CompareResult {
	subLogger := log.With().Str("kind", reflect.TypeOf(desired).String()).Str("name", api.GetName(desired)).Logger()

	actual, err := apiClient.Get(desired)
	if err != nil {
		errStatus, ok := status.FromError(err)
		if ok && errStatus.Code() == codes.NotFound {
			subLogger.Info().Msg("resource not found on system, will create it")
			return Create
		} else {
			subLogger.Error().Err(err).Msg("issue checking resource, skipping it")
			return Error
		}
	}

	actualYaml, err := ProtoToYaml(actual)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue converting actual resource to yaml for diff, skipping it")
		return Error
	}
	desiredYaml, err := ProtoToYaml(desired)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue converting desired resource to yaml for diff, skipping it")
		return Error
	}
	if string(actualYaml) != string(desiredYaml) {
		subLogger.Info().Msg("resource different than version on system, will replace it")
		dmp := diffmatchpatch.New()
		diffs := dmp.DiffMain(string(actualYaml), string(desiredYaml), true)
		subLogger.Info().Msgf("Diff: %s", dmp.DiffPrettyText(diffs))
		return Replace
	} else {
		subLogger.Info().Msg("resource identical to version on system, will skip it")
		return Skip
	}
}
