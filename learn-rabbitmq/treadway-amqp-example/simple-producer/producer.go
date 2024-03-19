// This example declares a durable Exchange, and publishes a single message to
// that Exchange with a given routing key.
package main

import (
	"flag"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"strconv"
	"time"
)

var (
	uri          = flag.String("uri", "amqp://admin:admin@localhost:5672/test-virtual-host-1", "AMQP URI")
	exchangeName = flag.String("exchange", "test-exchange", "Durable AMQP exchange name")
	exchangeType = flag.String("exchange-type", "direct", "Exchange type - direct|fanout|topic|x-custom")
	routingKey   = flag.String("key", "test-key", "AMQP routing key")
	body         = flag.String("body", "foobar", "Body of message")
	reliable     = flag.Bool("reliable", true, "Wait for the publisher confirmation before exiting")
)

func init() {
	flag.Parse()
}

func main() {
	if err := publish(*uri, *exchangeName, *exchangeType, *routingKey, *body, *reliable); err != nil {
		log.Fatalf("%s", err)
	}
	log.Printf("published %dB OK", len(*body))
}

func publish(amqpURI, exchange, exchangeType, routingKey, body string, reliable bool) error {

	// This function dials, connects, declares, publishes, and tears down,
	// all in one go. In a real service, you probably want to maintain a
	// long-lived connection as state, and publish against that.

	log.Printf("dialing %q", amqpURI)
	connection, err := amqp.Dial(amqpURI)
	if err != nil {
		return fmt.Errorf("Dial: %s", err)
	}
	defer connection.Close()

	log.Printf("got Connection, getting Channel")
	channel, err := connection.Channel()
	if err != nil {
		return fmt.Errorf("Channel: %s", err)
	}
	//channel2, err2 := connection.Channel()
	//if err2 != nil {
	//	return fmt.Errorf("Channel2: %s", err2)
	//}

	log.Printf("got Channel, declaring %q Exchange (%q)", exchangeType, exchange)
	// 声明exchange(需要exchange名称，类型
	if err := channel.ExchangeDeclare(
		exchange,     // name //队列名
		exchangeType, // type //交换器类型
		true,         // durable // 是否持久化
		false,        // auto-deleted
		false,        // internal
		false,        // noWait
		nil,          // arguments
	); err != nil {
		return fmt.Errorf("Exchange Declare: %s", err)
	}

	// Reliable publisher confirms require confirm.select support from the
	// connection.
	if reliable {
		log.Printf("enabling publishing confirms.")
		if err := channel.Confirm(false); err != nil {
			return fmt.Errorf("Channel could not be put into confirm mode: %s", err)
		}

		confirms := channel.NotifyPublish(make(chan amqp.Confirmation, 1))

		go confirmOne(confirms)

		//confirm, c := channel.NotifyConfirm(make(chan uint64, 1), make(chan uint64, 1))
		//go func() {
		//	for c := range confirm {
		//		fmt.Println("c: ", c)
		//	}
		//}()
		//go func() {
		//	for u := range c {
		//		fmt.Println("u: ", u)
		//	}
		//}()
	}

	log.Printf("declared Exchange, publishing %dB body (%q)", len(body), body)
	if err = channel.Publish(
		exchange,   // publish to an exchange
		routingKey, // routing to 0 or more queues
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "text/plain",
			ContentEncoding: "",
			Body:            []byte(body),
			DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
			Priority:        0,              // 0-9
			// a bunch of application/implementation-specific fields
		},
	); err != nil {
		return fmt.Errorf("Exchange Publish: %s", err)
	}
	for i := 0; i < 10; i++ {
		time.Sleep(time.Second * 1)
		if err = channel.Publish(
			exchange,   // publish to an exchange
			routingKey, // routing to 0 or more queues
			false,      // mandatory
			false,      // immediate
			amqp.Publishing{
				Headers:         amqp.Table{},
				ContentType:     "text/plain",
				ContentEncoding: "",
				Body:            []byte(body + ": " + strconv.Itoa(i)),
				DeliveryMode:    amqp.Persistent, // 1=non-persistent, 2=persistent
				Priority:        0,               // 0-9
				// a bunch of application/implementation-specific fields
			},
		); err != nil {
			return fmt.Errorf("Exchange Publish: %s", err)
		}
	}
	return nil
}

// One would typically keep a channel of publishings, a sequence number, and a
// set of unacknowledged sequence numbers and loop until the publishing channel
// is closed.
func confirmOne(confirms <-chan amqp.Confirmation) {
	log.Printf("waiting for confirmation of one publishing")

	for c := range confirms {
		if c.Ack {
			log.Printf("confirmed delivery with delivery tag: %d", c.DeliveryTag)
		} else {
			// 消息发送到exchange失败，可以在这里记录日志，通知管理员处理
			log.Printf("failed delivery of delivery tag: %d", c.DeliveryTag)
		}
	}
	//if confirmed := <-confirms; confirmed.Ack {
	//	log.Printf("confirmed delivery with delivery tag: %d", confirmed.DeliveryTag)
	//} else {
	//	log.Printf("failed delivery of delivery tag: %d", confirmed.DeliveryTag)
	//}
}
