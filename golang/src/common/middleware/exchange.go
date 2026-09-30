package middleware

import (
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type exchangeMiddleware struct {
	commonMiddleware
	exchange string
	keys     []string
}

func (exchangeMiddleware *exchangeMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	exchangeMiddleware.channel.Qos(1, 0, false)

	anonymousQueue, err := exchangeMiddleware.channel.QueueDeclare(
		"",    // nombre vacío, genera uno al azar
		false, // durable
		true,  // autoDelete
		true,  // exclusive
		false, // noWait
		nil,   // sin funcionalidades específicas adicionales
	)
	if err != nil {
		return ErrMessageMiddlewareMessage
	}

	for _, key := range exchangeMiddleware.keys {
		err = exchangeMiddleware.channel.QueueBind(
			anonymousQueue.Name,
			key,
			exchangeMiddleware.exchange,
			false,
			nil,
		)
		if err != nil {
			return ErrMessageMiddlewareMessage
		}
	}

	exchangeMiddleware.consumerTag = fmt.Sprintf("consumer_%s_%d", anonymousQueue.Name, time.Now().UnixNano())

	messages, err := exchangeMiddleware.channel.Consume(
		anonymousQueue.Name,
		exchangeMiddleware.consumerTag,
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,
	)
	if err != nil {
		return ErrMessageMiddlewareMessage
	}

	for delivery := range messages {
		msg := Message{Body: string(delivery.Body)}
		deliveryTag := delivery.DeliveryTag
		ack := func() { exchangeMiddleware.channel.Ack(deliveryTag, false) }
		nack := func() { exchangeMiddleware.channel.Nack(deliveryTag, false, true) }
		callbackFunc(msg, ack, nack)
	}

	return nil
}

func (exchangeMiddleware *exchangeMiddleware) Send(msg Message, routingKey string) error {
	keysToPublish := exchangeMiddleware.keys
	if routingKey != "" {
		keysToPublish = []string{routingKey}
	}

	for _, key := range keysToPublish {
		err := exchangeMiddleware.channel.Publish(
			exchangeMiddleware.exchange, // exchange
			key,         // routing key
			false,       // mandatory
			false,       // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(msg.Body),
			})
		if err != nil {
			return ErrMessageMiddlewareMessage
		}
	}
	return nil
}
