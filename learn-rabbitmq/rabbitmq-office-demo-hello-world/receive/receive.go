package main

import (
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

	defer ch.Close()

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
	err = ch.Qos(
		2,
		0,
		false)
	if err != nil {
		panic(err)
	}
	msgs, err := ch.Consume(
		queue.Name,
		"",
		false,
		false,
		false,
		false,
		nil)
	if err != nil {
		panic(err)
	}

	var forever chan struct{}
	go func() {
		for msg := range msgs {
			go func(msg amqp.Delivery) {
				log.Printf("处理任务开始: delivery tag: %d, msg: %s \n", msg.DeliveryTag, msg.Body)
				time.Sleep(5 * time.Second)
				log.Printf("处理任务结束: delivery tag: %d, msg: %s \n", msg.DeliveryTag, msg.Body)
				msg.Ack(false)
			}(msg)
		}
	}()
	<-forever
}
