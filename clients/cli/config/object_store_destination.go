package config

import (
	"fmt"

	"github.com/kaskada-ai/kaskada/clients/cli/utils"
	"github.com/spf13/cobra"

	apiv1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
)

const objectStoreOutputPrefixUriHelp = `
The uri of where to push output to.
The kaskada service must have access to the destination.
(examples "s3://my-bucket/path/to/results/" or "file:///local/path/to/results/")
`

const objectStoreFileTypeHelp = `
The kind of file to output.
(one-of "csv" or "parquet")
`

type ObjectStoreDestination struct {

	// The kind of file to output.
	// Either csv, or parquet.
	FileType *string `json:"file_type"`

	// The uri of where to push output to.
	//
	// examples:
	// s3://my-bucket/path/to/results/
	// file:///local/path/to/results/
	OutputPrefixUri *string `json:"output_prefix_uri"`
}

func NewObjectStoreDestination() ObjectStoreDestination {
	return ObjectStoreDestination{
		FileType:        new(string),
		OutputPrefixUri: new(string),
	}
}

func (c ObjectStoreDestination) AddFlagsToCommand(cmd *cobra.Command) {
	cmd.Flags().StringVar(c.FileType, "object-store-file-type", "csv", utils.FormatHelp(objectStoreFileTypeHelp))
	cmd.Flags().StringVar(c.OutputPrefixUri, "object-store-output-prefix-uri", "", utils.FormatHelp(objectStoreOutputPrefixUriHelp))
}

func (c ObjectStoreDestination) ToProto() (*apiv1alpha.ObjectStoreDestination, error) {
	var fileType apiv1alpha.FileType
	switch *c.FileType {
	case "csv":
		fileType = apiv1alpha.FileType_FILE_TYPE_CSV
	case "parquet":
		fileType = apiv1alpha.FileType_FILE_TYPE_PARQUET
	default:
		return nil, fmt.Errorf("object-store-file-type must be one of: csv or parquet")
	}

	if *c.OutputPrefixUri == "" {
		return nil, fmt.Errorf("object-store-output-prefix-uri must be set")
	}

	return &apiv1alpha.ObjectStoreDestination{
		FileType:        fileType,
		OutputPrefixUri: *c.OutputPrefixUri,
	}, nil
}
