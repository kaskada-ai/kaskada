package client

import (
	"errors"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"
)

const (
	amqpReconnectDelay = 5 * time.Second
)

var (
	// connection already closed
	errAMQPConnectionClosed = errors.New("connection already closed")
)

// AMQPClientConfig is the information need to connect to an AMQP server
type AMQPClientConfig struct {
	Host     string
	Port     int
	Password string
	Username string
}

func (c AMQPClientConfig) connectionString() string {
	return fmt.Sprintf("amqp://%s:%s@%s:%d/", c.Username, c.Password, c.Host, c.Port)
}

type amqpConnection struct {
	channels        []*amqpChannel
	connection      *amqp.Connection
	done            chan bool
	isReady         bool
	notifyConnClose chan *amqp.Error
}

// NewAMQPConnectiont creates a new AMQPConnection instance, and automatically
// attempts to connect to the server.
func NewAMQPConnection(config AMQPClientConfig) AMQPConnection {
	connection := &amqpConnection{
		done:    make(chan bool),
		isReady: false,
	}
	go connection.handleReconnect(config.connectionString())
	return connection
}

// handleReconnect will wait for a connection error on
// notifyConnClose, and then continuously attempt to reconnect.
func (c *amqpConnection) handleReconnect(addr string) {
	subLogger := log.With().Str("method", "amqpConnection.handleReconnect").Logger()

	for {
		// attempt to make connection
		conn, err := amqp.Dial(addr)
		if err != nil {
			subLogger.Error().Err(err).Msg("failed to connect. retrying...")

			select {
			case <-c.done:
				return
			case <-time.After(amqpReconnectDelay):
			}
			continue
		}

		subLogger.Info().Msg("connected to amqp server")
		c.connection = conn

		// register connection close listener
		c.notifyConnClose = make(chan *amqp.Error, 1)
		c.connection.NotifyClose(c.notifyConnClose)

		c.isReady = true

		// after successful reconnect, re-init all channels
		for _, channel := range c.channels {
			go channel.handleReInit(c.connection)
		}

		select {
		case <-c.done:
			return
		case <-c.notifyConnClose:
			c.isReady = false
			subLogger.Info().Msg("connection closed. reconnecting...")
		}
	}
}

// Close will cleanly shutdown the connection and all channels
func (c *amqpConnection) Close() error {
	// if already closed, skip
	if !c.isReady {
		return errAMQPConnectionClosed
	}

	c.isReady = false
	close(c.done)

	for _, channel := range c.channels {
		channel.Close() // error returned here is already logged
	}

	if err := c.connection.Close(); err != nil {
		subLogger := log.With().Str("method", "amqpConnection.Close").Logger()
		subLogger.Error().Err(err).Msg("issue closing connection")
		return err
	}

	return nil
}

func (c *amqpConnection) GetNewChannel(queueName string) AMQPChannel {
	channel := &amqpChannel{
		queueName: queueName,
	}
	if c.isReady {
		go channel.handleReInit(c.connection)
	}
	c.channels = append(c.channels, channel)
	return channel
}
