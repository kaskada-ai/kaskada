package client

import (
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"net/url"
	go_os "os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/c2fo/vfs/v6"
	"github.com/c2fo/vfs/v6/backend"
	"github.com/c2fo/vfs/v6/backend/azure"
	"github.com/c2fo/vfs/v6/backend/gs"
	"github.com/c2fo/vfs/v6/backend/mem"
	"github.com/c2fo/vfs/v6/backend/os"
	"github.com/c2fo/vfs/v6/backend/s3"
	vfs_utils "github.com/c2fo/vfs/v6/utils"
	"github.com/kaskada-ai/kaskada/wren/utils"

	"github.com/rs/zerolog/log"
)

const (
	object_store_type_local = "local"
	object_store_type_s3    = "s3"
	object_store_type_gcs   = "gcs"
	object_store_type_azure = "azure"
)

type Object struct {
	uri  string
	path string
}

func (o Object) URI() string {
	return o.uri
}

func (o Object) Path() string {
	return o.path
}

// NewObjectStoreClient creates a new ObjectStoreClient
func NewObjectStoreClient(env string, objectStoreType string, bucket string, path string, endpoint string, disableSSL bool, forcePathStyle bool) ObjectStoreClient {
	objectStoreType = strings.ToLower(objectStoreType)

	switch objectStoreType {
	case object_store_type_local:
		absPath, err := filepath.Abs(path)
		if err != nil {
			log.Fatal().Msgf("could not locate local data path: %s", path)
		}
		path = vfs_utils.EnsureTrailingSlash(absPath)
		log.Debug().Msgf("objectStorePath: %s", path)
	case object_store_type_azure, object_store_type_gcs, object_store_type_s3:
		if bucket == "" {
			log.Fatal().Msgf("when using %s for the `object-store-type`, `object-store-bucket` is requried.", objectStoreType)
		} else if !filepath.IsAbs(path) {
			log.Fatal().Msgf("when using %s for the `object-store-type`, `object-store-path` cannot be a relative path or prefix", objectStoreType)
		}
		path = vfs_utils.EnsureTrailingSlash(path)
	default:
		log.Fatal().Msg("invalid value set for `object-store-type`. Should be  `local`, `s3`, `gcs`, or `azure`")
	}

	var (
		rootObjectStore vfs.FileSystem
		awsS3           s3iface.S3API
	)

	switch objectStoreType {
	case object_store_type_azure:
		rootObjectStore = backend.Backend(azure.Scheme)
	case object_store_type_gcs:
		rootObjectStore = backend.Backend(gs.Scheme)
	case object_store_type_local:
		rootObjectStore = backend.Backend(os.Scheme)
	case object_store_type_s3:
		//vfs client config
		s3Options := s3.Options{
			DisableServerSideEncryption: disableSSL,
			ForcePathStyle:              forcePathStyle,
		}

		// aws s3 client config
		awsConfig := aws.NewConfig().WithDisableSSL(disableSSL).WithS3ForcePathStyle(forcePathStyle)

		// config endoint
		if endpoint != "" {
			s3Options.Endpoint = endpoint
			awsConfig = awsConfig.WithEndpoint(endpoint)
		}

		// create vfs client
		rootObjectStore = s3.NewFileSystem().WithOptions(s3Options)

		// create aws s3 client
		opts := session.Options{
			SharedConfigState: session.SharedConfigEnable,
			Config:            *awsConfig,
		}

		sess, err := session.NewSessionWithOptions(opts)
		if err != nil {
			log.Fatal().Err(err).Msg("not able to initialize a new s3 session to aws")
		}
		awsS3 = aws_s3.New(sess)

	default:
		log.Fatal().Msg("invalid value set for `object-store-type`. Should be  `local`, `s3`, `gcs`, or `azure`")
	}

	dataLocation, err := rootObjectStore.NewLocation(bucket, path)
	if err != nil {
		log.Fatal().Err(err).Msg("unable to initialize object store")
	}

	return objectStoreClient{
		objectStoreType: objectStoreType,
		dataLocation:    dataLocation,
		awsS3:           awsS3,
	}
}

type objectStoreClient struct {
	objectStoreType string
	dataLocation    vfs.Location
	awsS3           s3iface.S3API
}

// copies an object into our object store
// returns the URI of the object in our object store
func (c objectStoreClient) CopyObjectIn(ctx context.Context, fromURI string, toPath string) (newObject Object, err error) {
	subLogger := log.Ctx(ctx).With().Str("method", "objectStoreClient.CopyObjectIn").Str("fromURI", fromURI).Str("toPath", toPath).Logger()

	var fromFile, toFile vfs.File
	fromFile, err = newFile(fromURI)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue accessing fromURI")
		return
	}
	defer fromFile.Close()

	toFile, err = c.dataLocation.NewFile(toPath)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue accessing toPath")
		return
	}
	defer toFile.Close()

	newObject = Object{
		uri:  toFile.URI(),
		path: strings.TrimPrefix(toFile.Path(), c.dataLocation.Path()),
	}

	if c.objectStoreType == object_store_type_local {
		err = go_os.MkdirAll(vfs_utils.EnsureTrailingSlash(toFile.Location().Path()), go_os.ModePerm)
		if err != nil {
			subLogger.Error().Err(err).Str("toURI", newObject.uri).Msg("issue creating directory for file")
		}
	}

	err = fromFile.CopyToFile(toFile)
	if err != nil {
		subLogger.Error().Err(err).Str("toURI", newObject.uri).Msg("issue copying file")
	}
	return
}

// deletes the object at the provided path from our object store
func (c objectStoreClient) DeleteObject(ctx context.Context, object Object) error {
	subLogger := log.Ctx(ctx).With().Str("method", "objectStoreClient.DeleteObject").Str("path", object.path).Logger()

	file, err := c.dataLocation.NewFile(object.path)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue accessing path")
		return err
	}

	err = file.Delete()
	if err != nil {
		subLogger.Error().Err(err).Msg("issue deleting path")
		return err
	}
	return nil
}

// deletes all objects under the provided path from our object store
func (c objectStoreClient) DeleteObjects(ctx context.Context, subPath string) error {
	subLogger := log.Ctx(ctx).With().Str("method", "objectStoreClient.DeleteObjects").Str("path", subPath).Logger()

	objectList, err := c.dataLocation.ListByPrefix(subPath)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue list objects")
		return err
	}
	for _, object := range objectList {
		err = c.dataLocation.DeleteFile(object)
		if err != nil {
			subLogger.Error().Err(err).Str("path", object).Msg("issue deleting object")
			return err
		}
	}
	return nil
}

// generates a presigned URL to download an object from our store.
// Note the signing step will be skipped when the object-store-type is `local`
func (c objectStoreClient) GetPresignedDownloadURL(ctx context.Context, URI string) (presignedURL string, err error) {
	subLogger := log.Ctx(ctx).With().Str("method", "objectStoreClient.GetPresignedDownloadURL").Str("uri", URI).Logger()

	var file vfs.File
	file, err = newFile(URI)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue accessing URI")
		return
	}

	switch c.objectStoreType {
	case object_store_type_local:
		presignedURL = file.Path()
		return

	case object_store_type_s3:
		getObjectInput := &aws_s3.GetObjectInput{
			Bucket: aws.String(file.Location().Volume()),
			Key:    aws.String(file.Path()),
		}
		req, _ := c.awsS3.GetObjectRequest(getObjectInput)
		presignedURL, err = req.Presign(60 * time.Minute)
		if err != nil {
			subLogger.Error().Err(err).Msg("issue generating presigned download url for uri")
		}
		return

	default:
		subLogger.Error().Str("type", c.objectStoreType).Msg("presigning download URLs is unimplemented for this object-store-type")
		err = fmt.Errorf("presigning download URLs is unimplemented for object-store-type: %s", c.objectStoreType)
		return
	}
}

// gets an MD5 hash or equivalent identifier for an object in our store
func (c objectStoreClient) GetObjectIdentifier(ctx context.Context, object Object) (identifier string, err error) {
	subLogger := log.Ctx(ctx).With().Str("method", "objectStoreClient.GetObjectIdentifier").Str("path", object.path).Logger()

	var file vfs.File
	file, err = c.dataLocation.NewFile(object.path)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue accessing path")
		return
	}
	defer file.Close()

	switch c.objectStoreType {
	case object_store_type_local:
		h := md5.New()
		if _, err = io.Copy(h, file); err != nil {
			subLogger.Error().Err(err).Msg("issue getting object indentifier")
			return
		}
		identifier = fmt.Sprintf("%x", h.Sum(nil))
		return

	case object_store_type_s3:
		headObjectInput := &aws_s3.HeadObjectInput{
			Bucket: aws.String(file.Location().Volume()),
			Key:    aws.String(file.Path()),
		}

		var result *aws_s3.HeadObjectOutput
		result, err = c.awsS3.HeadObjectWithContext(ctx, headObjectInput)
		if err != nil {
			subLogger.Error().Err(err).Msg("issue getting object indentifier")
			return
		}
		identifier = utils.TrimQuotes(*result.ETag)
		return

	default:
		subLogger.Error().Str("type", c.objectStoreType).Msg("getting an object identifier is unimplemented for this object-store-type")
		err = fmt.Errorf("getting an object identifier is unimplemented for object-store-type: %s", c.objectStoreType)
		return
	}
}

// returns the absolute URI of a path inside our object store
func (c objectStoreClient) GetDataPathURI(subPath string) string {
	if c.objectStoreType == object_store_type_local {
		absPath := c.dataLocation.Path() + subPath
		go_os.MkdirAll(absPath, go_os.ModePerm)
	}

	return c.dataLocation.URI() + subPath
}

/*
---- below is based on: https://github.com/C2FO/vfs/blob/master/vfssimple/vfssimple.go ----
*/

var (
	ErrMissingAuthority = errors.New("unable to determine uri authority ([user@]host[:port]) for network-based scheme")
	ErrMissingScheme    = errors.New("unable to determine uri scheme")
	ErrRegFsNotFound    = errors.New("no matching registered filesystem found")
	ErrBlankURI         = errors.New("uri is blank")
)

// NewFile is a convenience function that allows for instantiating a file based on a uri string. Any
// backend file system is supported, though some may require prior configuration. See the docs for
// specific requirements of each.
func newFile(uri string) (vfs.File, error) {
	fs, host, path, err := parseSupportedURI(uri)
	if err != nil {
		return nil, fmt.Errorf("unable to create vfs.File for uri %q: %w", uri, err)
	}

	return fs.NewFile(host, path)
}

// parseURI attempts to parse a URI and validate that it returns required results
func parseURI(uri string) (scheme, authority, path string, err error) {
	// return early if blank uri
	if uri == "" {
		err = ErrBlankURI
		return
	}

	// parse URI
	var u *url.URL
	u, err = url.Parse(uri)
	if err != nil {
		err = fmt.Errorf("unknown url.Parse error: %w", err)
		return
	}

	// validate schema
	scheme = u.Scheme
	if u.Scheme == "" {
		err = ErrMissingScheme
		return
	}

	// validate authority
	authority = u.Host
	path = u.Path
	if azure.IsValidURI(u) {
		authority, path, err = azure.ParsePath(path)
	}

	if u.User.String() != "" {
		authority = fmt.Sprintf("%s@%s", u.User, u.Host)
	}
	// network-based schemes require authority, but not file:// or mem://
	if authority == "" && !(scheme == os.Scheme || scheme == mem.Scheme) {
		return "", "", "", ErrMissingAuthority
	}

	return
}

// parseSupportedURI checks if URI matches any backend name as prefix, capturing the longest(most specific) match found.
// See doc.go Registered Backend Resoltion seciton for examples.
func parseSupportedURI(uri string) (vfs.FileSystem, string, string, error) {
	_, authority, path, err := parseURI(uri)
	if err != nil {
		return nil, "", "", err
	}

	var longest string
	backends := backend.RegisteredBackends()
	for _, backendName := range backends {
		if strings.HasPrefix(uri, backendName) {
			// The first match always becomes the longest
			if longest == "" {
				longest = backendName
				continue
			}

			// we found a longer (more specific) backend prefix matching URI
			if len(backendName) > len(longest) {
				longest = backendName
			}
		}
	}

	if longest == "" {
		err = ErrRegFsNotFound
	}

	return backend.Backend(longest), authority, path, err
}
