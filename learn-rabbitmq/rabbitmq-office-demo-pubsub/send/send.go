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

	exchangeName := "exchange-logs"
	exchangeType := "fanout"
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

	if err := ch.Confirm(false); err != nil {
		panic(err)
	}
	//confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	//
	//go func() {
	//	for confirm := range confirms {
	//		if confirm.Ack {
	//			fmt.Printf("confirmed delivery with delivery tag: %d \n", confirm.DeliveryTag)
	//		} else {
	//			fmt.Printf("confirmed delivery of delivery tag: %d \n", confirm.DeliveryTag)
	//		}
	//	}
	//}()
	// 发送消息
	for i := 0; i < 10; i++ {
		body := fmt.Sprintf("log info, %d", i)

		//ch.Tx()
		//err = ch.PublishWithContext(
		//	ctx,
		//	exchangeName,
		//	"",
		//	false,
		//	false,
		//	amqp.Publishing{
		//		ContentType:  "text/plain",
		//		Body:         []byte(body),
		//		DeliveryMode: amqp.Persistent,
		//		//AppId:         "appid",
		//		//UserId:        "admin",
		//		//MessageId:     "messageid",
		//		//CorrelationId: "correlationid",
		//	},
		//)

		//ch.TxCommit()
		deferredConfirmation, err := ch.PublishWithDeferredConfirmWithContext(
			ctx,
			exchangeName,
			"",
			false,
			false,
			amqp.Publishing{
				Headers:      map[string]interface{}{},
				ContentType:  "text/plain",
				Body:         []byte(body),
				DeliveryMode: amqp.Persistent,
				Expiration:   "10000",
				//AppId:         "appid",
				//UserId:        "admin",
				//MessageId:     "messageid",
				//CorrelationId: "correlationid",
			},
		)
		if err != nil {
			panic(err)
		}
		//fmt.Println("deferredConfirmation.DeliveryTag ", deferredConfirmation.DeliveryTag)
		//fmt.Println("deferredConfirmation.Acked() ", deferredConfirmation.Acked())
		//done := <-deferredConfirmation.Done()
		//fmt.Println("done!!!! ", done)
		//fmt.Println("deferredConfirmation.Acked() ", deferredConfirmation.Acked())

		fmt.Println("deferredConfirmation.Acked() ", deferredConfirmation.Acked())
		ok := deferredConfirmation.Wait()
		fmt.Println("deferredConfirmation.Acked() ", deferredConfirmation.Acked())
		fmt.Println("wait ok: ", ok)
		time.Sleep(1 * time.Second)

	}

	log.Println("send message success")
	time.Sleep(2 * time.Second)
}
