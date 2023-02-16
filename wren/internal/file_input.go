package internal

import (
	"github.com/kaskada/kaskada-ai/wren/ent/kaskadafile"
	v1alpha "github.com/kaskada/kaskada-ai/wren/gen/kaskada/kaskada/v1alpha"
)

type fileInput struct {
	uri      string
	fileType kaskadafile.Type
}

func FileInputFromV1Alpha(input *v1alpha.FileInput) FileInput {
	var fileType kaskadafile.Type

	switch input.FileType {
	case v1alpha.FileType_FILE_TYPE_CSV:
		fileType = kaskadafile.TypeCsv
	case v1alpha.FileType_FILE_TYPE_PARQUET:
		fileType = kaskadafile.TypeParquet
	default:
		fileType = kaskadafile.TypeUnspecified
	}

	return fileInput{
		uri:      input.Uri,
		fileType: fileType,
	}
}

func (f fileInput) GetURI() string {
	return f.uri
}

func (f fileInput) GetType() kaskadafile.Type {
	return f.fileType
}

func (f fileInput) GetExtension() string {
	switch f.fileType {
	case kaskadafile.TypeCsv:
		return "csv"
	case kaskadafile.TypeParquet:
		return "parquet"
	default:
		return "undefined"
	}
}

func (f fileInput) String() string {
	return f.uri
}
