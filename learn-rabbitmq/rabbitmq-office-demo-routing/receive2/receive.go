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

	// channel
	ch, err := conn.Channel()
	if err != nil {
		log.Printf("failed to open a channel %s", err)
	}

	defer ch.Close()
	// producer和consumer都需要声明exchange, 如果在producer声明，在consumer没声明，先启动consumer的话，会报错
	// panic: Exception (404) Reason: "NOT_FOUND - no exchange 'exchange-logs' in vhost
	// 'test-virtual-host-1'"
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

	queue, err := ch.QueueDeclare(
		"routing-queue-log-2", // name为""时，会自动生成一个随机name的queue. 不同name时consumer才会都消费所有消息，相同queue name的consumer会依次消费消息
		false,
		false,
		false,
		false,
		nil)
	if err != nil {
		panic(err)
	}
	// 绑定queue和exchange和route key
	err = ch.QueueBind(
		queue.Name,
		"critical-log", // 对 fanout 模式无效
		exchangeName,
		false,
		nil)
	if err != nil {
		panic(err)
	}

	msgs, err := ch.Consume(
		queue.Name,
		"consumer-name",
		true,
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
			//go func(msg amqp.Delivery) {
			log.Printf("处理critical log 任务: delivery tag: %d, msg: %s \n", msg.DeliveryTag, msg.Body)
			//time.Sleep(5 * time.Second)
			//log.Printf("处理任务结束: delivery tag: %d, msg: %s \n", msg.DeliveryTag, msg.Body)
			//msg.Ack(false)
			//}(msg)
		}
	}()
	<-forever
}
