package middleware

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type commonMiddleware struct{
	connection *amqp.Connection
	channel *amqp.Channel
	consumerTag string
}

func (commonMiddleware *commonMiddleware) StopConsuming() error {
	if commonMiddleware.consumerTag != "" {
		if err := commonMiddleware.channel.Cancel(commonMiddleware.consumerTag, false); err != nil {
			return ErrMessageMiddlewareDisconnected
		}
	}
	return nil
}

func (commonMiddleware *commonMiddleware) Close() error {
	if commonMiddleware.channel != nil {
		commonMiddleware.channel.Close()
	}

	if commonMiddleware.connection != nil {
		if err := commonMiddleware.connection.Close(); err != nil {
			return ErrMessageMiddlewareClose
		}
	}

	return nil
}

func (commonMiddleware *commonMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	return nil
}

func (commonMiddleware *commonMiddleware) Send(msg Message) error{
	return nil
}

