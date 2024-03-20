package main

import (
	"context"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"time"
)

func main() {
	// 连接
	conn, err := amqp.Dial("amqp://admin:admin@localhost:5672/test-virtual-host-1")
	if err != nil {
		log.Panicf("failed to connect to rabbitmq %s", err)
	}
	defer conn.Close()

	// channel
	ch, err := conn.Channel()
	if err != nil {
		log.Printf("failed to open a channel %s", err)
	}

	defer ch.Close()
	// 声明延迟exchange
	delayExchangeName := "delay_message_exchange"
	err = ch.ExchangeDeclare(
		delayExchangeName,
		"x-delayed-message", // 类型为 x-delayed-message
		true,
		false,
		false,
		false,
		map[string]interface{}{
			"x-delayed-type": "direct",
		},
	)
	if err != nil {
		panic(err)
	}

	for i := 1; i < 5; i++ {
		err := ch.PublishWithContext(
			context.Background(),
			delayExchangeName,
			"aa",
			false,
			false,
			amqp.Publishing{
				Headers: map[string]interface{}{
					"x-delay": 5000,
				},
				ContentType:  "text/plain",
				Body:         []byte("2333"),
				DeliveryMode: amqp.Persistent,
				Timestamp:    time.Now(),
				//Expiration:   "5000",
			},
		)
		if err != nil {
			panic(err)
		}
	}
}
