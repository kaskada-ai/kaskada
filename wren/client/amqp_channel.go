package client

import (
	"context"
	"errors"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"
)

const (
	amqpReInitDelay = 2 * time.Second
	amqpResendDelay = 5 * time.Second
	amqpSendTimeout = 1 * time.Second
)

var (
	// channel not ready
	errAMQPChannelNotReady = errors.New("channel not ready")

	// channel already closed
	errAMQPChannelClosed = errors.New("channel already closed")

	// channel shutdown started
	errAMQPChannelShutdown = errors.New("channel shutdown started")
)

type amqpChannel struct {
	channel         *amqp.Channel
	done            chan bool
	isReady         bool
	notifyConnClose chan *amqp.Error
	notifyChanClose chan *amqp.Error
	notifyConfirm   chan amqp.Confirmation
	queueName       string
}

// handleReInit will wait for a channel error and then attempt to re-initialize
// the channel. it will quit on a connection error.
func (c *amqpChannel) handleReInit(conn *amqp.Connection) {
	subLogger := log.With().Str("method", "amqpChannel.handleReInit").Logger()

	c.isReady = false
	c.done = make(chan bool)
	c.notifyConnClose = make(chan *amqp.Error, 1)
	conn.NotifyClose(c.notifyConnClose)

	for {
		err := c.init(conn)
		if err != nil {
			subLogger.Error().Err(err).Msg("failed to initialize channel. retrying...")

			select {
			case <-c.done:
				return
			case <-time.After(amqpReInitDelay):
			}
			continue
		}

		select {
		case <-c.done:
			return
		case <-c.notifyConnClose:
			subLogger.Info().Msg("connection closed. exiting re-init go-routine")
			c.isReady = false
			return
		case <-c.notifyChanClose:
			subLogger.Info().Msg("channel closed. re-running init...")
			c.isReady = false
			<-time.After(amqpReInitDelay)
		}
	}
}

// init will initialize channel & declare queue
func (c *amqpChannel) init(conn *amqp.Connection) error {
	subLogger := log.With().Str("method", "amqpChannel.init").Logger()

	ch, err := conn.Channel()
	if err != nil {
		subLogger.Error().Err(err).Msg("issue initializing new channel")
		return err
	}

	closeChan := func() {
		err := ch.Close()
		subLogger.Error().Err(err).Msg("issue closing new channel")
	}

	err = ch.Confirm(false)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue putting new channel in confirm mode")
		closeChan()
		return err
	}
	_, err = ch.QueueDeclare(
		c.queueName,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		subLogger.Error().Err(err).Str("queue_name", c.queueName).Msg("issue declaring queue")
		closeChan()
		return err
	}

	subLogger.Info().Msg("initialized new channel")
	c.channel = ch

	// register channel close and confirm listeners
	c.notifyChanClose = make(chan *amqp.Error, 1)
	c.notifyConfirm = make(chan amqp.Confirmation, 1)
	c.channel.NotifyClose(c.notifyChanClose)
	c.channel.NotifyPublish(c.notifyConfirm)

	c.isReady = true
	subLogger.Info().Msg("channel and queue setup successfully")

	return nil
}

// Push will push data onto the queue, and wait for a confirm.
// If no confirms are received until within the resendTimeout,
// it continuously re-sends messages until a confirm is received.
// This will block until the server sends a confirm.
func (c *amqpChannel) Push(data []byte) error {
	subLogger := log.With().Str("method", "amqpChannel.Push").Logger()

	if !c.isReady {
		return errAMQPChannelNotReady
	}
	for {
		ctx, cancel := context.WithTimeout(context.Background(), amqpSendTimeout)
		defer cancel()

		err := c.channel.PublishWithContext(
			ctx,
			"",
			c.queueName,
			false,
			false,
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        data,
			},
		)

		if err != nil {
			subLogger.Error().Err(err).Msg("push failed. retrying...")
			select {
			case <-c.done:
				return errAMQPChannelShutdown
			case <-time.After(amqpResendDelay):
			}
			continue
		}
		select {
		case confirm := <-c.notifyConfirm:
			if confirm.Ack {
				subLogger.Debug().Msg("push confirmed")
				return nil
			}
		case <-time.After(amqpResendDelay):
		}
		subLogger.Debug().Msg("push didn't confirm. retrying...")
	}
}

// Consume will continuously put queue items on the channel.
// It is required to call delivery.Ack when it has been
// successfully processed, or delivery.Nack when it fails.
// Ignoring this will cause data to build up on the server.
func (c *amqpChannel) Consume() (<-chan amqp.Delivery, error) {
	subLogger := log.With().Str("method", "amqpChannel.Consume").Logger()

	if !c.isReady {
		return nil, errAMQPChannelNotReady
	}

	if err := c.channel.Qos(
		1,
		0,
		false,
	); err != nil {
		subLogger.Error().Err(err).Msg("issue setting qos on channel")
		return nil, err
	}

	return c.channel.Consume(
		c.queueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
}

// Close will cleanly shutdown the channel
func (c *amqpChannel) Close() error {
	// if already closed, skip
	if !c.isReady {
		return errAMQPChannelClosed
	}

	c.isReady = false
	close(c.done)

	if err := c.channel.Close(); err != nil {
		subLogger := log.With().Str("method", "amqpChannel.Close").Logger()
		subLogger.Error().Err(err).Msg("issue closing channel")
		return err
	}

	return nil
}

func (c *amqpChannel) NotifyClose(errChan chan *amqp.Error) (chan *amqp.Error, error) {
	if !c.isReady {
		return nil, errAMQPChannelNotReady
	}
	return c.channel.NotifyClose(errChan), nil
}

func (c *amqpChannel) IsReady() bool {
	return c.isReady
}
