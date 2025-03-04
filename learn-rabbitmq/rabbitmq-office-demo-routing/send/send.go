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

	exchangeName := "exchange-logs2"
	exchangeType := "direct"
	err = ch.ExchangeDeclare(
		exchangeName,
		exchangeType,
		true,
		false,
		false,
		false,
		nil)
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// 发送消息
	for i := 0; i < 20; i++ {
		body := fmt.Sprintf("log info, %d", i)

		key := "normal-log"
		//key := "critical-log"
		//if i >= 10 && i%2 == 0 {
		//	key = "critical-log"
		//}
		//if i < 10 && i%2 == 0 {
		//	key = "normal-log-2"
		//}
		err = ch.PublishWithContext(
			ctx,
			exchangeName,
			key,
			false,
			false,
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(body),
				//AppId:         "appid",
				//UserId:        "admin",
				//MessageId:     "messageid",
				//CorrelationId: "correlationid",
			},
		)
		if err != nil {
			panic(err)
		}
	}

	log.Println("send message success")
}
