package main

import (
	"context"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
)

func main() {
	// 连接
	conn, err := amqp.Dial("amqp://admin:admin@localhost:5672/test-virtual-host-1")
	if err != nil {
		log.Panicf("failed to connect to rabbitmq %s", err)
	}

	// channel
	ch, err := conn.Channel()
	if err != nil {
		log.Printf("failed to open a channel %s", err)
	}

	// 首先需要设置死信队列的 exchange 和 queue，然后进行绑定:

	deadLetterExchangeName := "dead_exchange"
	err = ch.ExchangeDeclare(
		deadLetterExchangeName,
		"fanout",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}

	deadLetterQueueName := "dead_queue"
	deadLetterQueue, err := ch.QueueDeclare(
		deadLetterQueueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}
	err = ch.QueueBind(deadLetterQueue.Name, "", deadLetterExchangeName, false, nil)
	if err != nil {
		panic(err)
	}

	exchangeName := "will_test_dead_letter_exchange"
	err = ch.ExchangeDeclare(
		exchangeName,
		"fanout",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}

	for i := 1; i < 10; i++ {
		err := ch.PublishWithContext(context.Background(),
			exchangeName,
			"",
			false,
			false,
			amqp.Publishing{
				ContentType:  "text/plain",
				Body:         []byte("2333"),
				DeliveryMode: amqp.Persistent,
				Expiration:   "5000",
			},
		)
		if err != nil {
			panic(err)
		}
	}

}
