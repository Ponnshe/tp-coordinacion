package middleware

import (
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const prefetchCount = 1
const prefetchSize = 0

type queueMiddleware struct {
	commonMiddleware
	queueName string
}

func (queueMiddleware *queueMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	queueMiddleware.channel.Qos(prefetchCount, prefetchSize, false)

	queueMiddleware.consumerTag = fmt.Sprintf("consumer %s %d", queueMiddleware.queueName, time.Now().UnixNano())

	messages, err := queueMiddleware.channel.Consume(
		queueMiddleware.queueName,
		queueMiddleware.consumerTag,
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return ErrMessageMiddlewareMessage
	}

	for delivery := range messages {
		msg := Message{Body: string(delivery.Body)}
		deliveryTag := delivery.DeliveryTag
		ack := func() { queueMiddleware.channel.Ack(deliveryTag, false)}
		nack := func() { queueMiddleware.channel.Nack(deliveryTag, false, true) }
		callbackFunc(msg, ack, nack)
	}

	return nil
}

func (queueMiddleware *queueMiddleware) Send(msg Message, routingKey string) error{
	err := queueMiddleware.channel.Publish(
		"",           // exchange (vacío significa default exchange)
		queueMiddleware.queueName, // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg.Body),
		})
	if err != nil {
		return ErrMessageMiddlewareMessage
	}
	return nil
}
