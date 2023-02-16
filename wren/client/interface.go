package client

import (
	"context"

	v1alpha "github.com/kaskada/kaskada-ai/wren/gen/kaskada/kaskada/v1alpha"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Closeable defines a closable client
type Closeable interface {
	Close() error
}

// wraps an AMQP connection
type AMQPConnection interface {
	// Close will cleanly shutdown the connection and any existing channels
	Close() error

	// Gets a new channel on the connection, and initializes a queue
	// This channel will close and re-init itself if an error occurs
	GetNewChannel(queueName string) AMQPChannel
}

type AMQPChannel interface {
	// Close will cleanly shutdown the channel
	Close() error

	// Consume will continuously put queue items on the channel.
	// It is required to call delivery.Ack when it has been
	// successfully processed, or delivery.Nack when it fails.
	// Ignoring this will cause data to build up on the server.
	Consume() (<-chan amqp.Delivery, error)

	// Push will push data onto the queue, and wait for a confirm.
	// If no confirms are received until within the resendTimeout,
	// it continuously re-sends messages until a confirm is received.
	// This will block until the server sends a confirm. Errors are
	// only returned if the push action itself fails.
	Push(data []byte) error

	// returns true when the channel is ready for consuming or pushing
	IsReady() bool

	// NotifyClose registers a listener for when the server sends a
	// channel exception in the form of a Channel.Close method. Channel
	// exceptions will only be broadcast to listeners on this channel.
	//
	// The chan provided will be closed when the Channel is closed and
	// on a graceful close, no error will be sent.
	//
	// In case of a non graceful close the error will be notified
	// synchronously by the library so that it will be necessary to
	// consume the Channel from the caller in order to avoid deadlocks
	NotifyClose(c chan *amqp.Error) (chan *amqp.Error, error)
}

// ComputeServiceClient is a Closable ComputeService client
type ComputeServiceClient interface {
	Closeable
	v1alpha.ComputeServiceClient
}

// ComputeServiceClientProvider creates ComputeServiceClients
type ComputeServiceClientProvider func(ctx context.Context) ComputeServiceClient

// FileServiceClient is a Closable FileService client
type FileServiceClient interface {
	Closeable
	v1alpha.FileServiceClient
}

// FileServiceClientProvider creates FileServiceClients
type FileServiceClientProvider func(ctx context.Context) FileServiceClient

// ObjectStoreClient is a Closable FileSystem client
type ObjectStoreClient interface {
	// copies an object into our object store
	// returns the URI of the object in our object store
	CopyObjectIn(ctx context.Context, inputFile string, toPath string) (Object, error)

	// deletes the provided object from our store
	DeleteObject(ctx context.Context, object Object) error

	// deletes all objects under the provided path from our object store
	DeleteObjects(ctx context.Context, path string) error

	// generates a presigned URL to download an object from our store.
	// Note the signing step will be skipped when the object-store-type is `local`
	GetPresignedDownloadURL(ctx context.Context, URI string) (string, error)

	// gets an MD5 hash or equivalent identifier for an object in our store
	GetObjectIdentifier(ctx context.Context, object Object) (string, error)

	// returns the absolute URI of a path inside our object store
	GetDataPathURI(path string) string
}

// ObjectStoreClientProvider creates ObjectStoreClients
type ObjectStoreClientProvider func(ctx context.Context) ObjectStoreClient

// PrepareServiceClient is a Closable PrepareService client
type PrepareServiceClient interface {
	Closeable
	v1alpha.PreparationServiceClient
}

// PrepareServiceClientProvider creates PrepareServiceClients
type PrepareServiceClientProvider func(ctx context.Context) PrepareServiceClient
