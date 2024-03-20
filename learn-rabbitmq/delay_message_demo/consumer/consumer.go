package main

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
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
	//exchangeName := "will_test_dead_letter_exchange"
	ququeName := "delay_message_queue"
	delayMessageExchangeName := "delay_message_exchange"

	queue, err := ch.QueueDeclare(
		ququeName, // name为""时，会自动生成一个随机name的queue. 不同name时consumer才会都消费所有消息，相同queue name的consumer会依次消费消息
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}

	err = ch.QueueBind(
		queue.Name,
		"aa",
		delayMessageExchangeName,
		false,
		nil)
	if err != nil {
		panic(err)
	}

	msgs, err := ch.Consume(
		queue.Name,
		"consumer-name",
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
			log.Printf("处理消息: %d, msg: %s \n", msg.DeliveryTag, msg.Body)
			msg.Ack(false)
		}
	}()
	<-forever
}
