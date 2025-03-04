package main

import (
	"context"
	"fmt"
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

	// channel
	ch, err := conn.Channel()
	if err != nil {
		log.Printf("failed to open a channel %s", err)
	}

	// 声明队列
	queue, err := ch.QueueDeclare(
		"hello",
		true,
		false,
		false,
		false,
		nil)

	if err != nil {
		panic(err)
	}

	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	go func() {
		for confirm := range confirms {
			log.Printf("消息发送确认, tag: %d, status: %s\n", confirm.DeliveryTag, confirm.Ack)
		}
	}()

	// 发送消息
	for i := 0; i < 10; i++ {
		body := fmt.Sprintf("Hello World! %d", i)

		err = ch.PublishWithContext(context.Background(),
			"",
			queue.Name,
			false,
			false,
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(body),
				//AppId:         "appid",
				//UserId:        "admin",
				MessageId:     "messageid",
				CorrelationId: "correlationid",
			},
		)
		if err != nil {
			panic(err)
		}
	}

	log.Println("send message success")

	time.Sleep(4 * time.Second)
}
