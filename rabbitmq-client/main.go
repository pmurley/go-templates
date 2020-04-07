package main

import (
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"time"
)

type Consumer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	tag     string
	done    chan error
}

type Task struct {
	Param1 string `json:"param_1"`
	Param2 string `json:"param_2"`
}

type Result struct {
	Param3 string `json:"param_3"`
	Param4 int    `json:"param_4"`
}

const NUM_WORKERS = 5
const RABBITMQ_USER = ""
const RABBITMQ_PASS = ""
const RABBITMQ_URL = ""
const RABBITMQ_INPUT_QUEUE = ""
const RABBITMQ_OUTPUT_QUEUE = ""
const rabbitURI = "amqp://" + RABBITMQ_USER + ":" +
	RABBITMQ_PASS + "@" + RABBITMQ_URL

func main() {

	c := &Consumer{
		conn:    nil,
		channel: nil,
		tag:     "",
		done:    make(chan error),
	}

	var err error
	c.conn, err = amqp.Dial(rabbitURI)
	if err != nil {
		log.Fatal(err)
	}
	defer c.conn.Close()

	c.channel, err = c.conn.Channel()
	if err != nil {
		log.Fatal(err)
	}

	err = c.channel.Qos(1, 0, true)
	if err != nil {
		log.Fatal(err)
	}

	queue, err := c.channel.QueueDeclarePassive(
		RABBITMQ_INPUT_QUEUE, // name of the queue
		true,                 // durable
		false,                // delete when unused
		false,                // exclusive
		false,                // noWait
		nil,                  // arguments
	)
	if err != nil {
		log.Fatal(err)
	}

	// Creates a new channel where deliveries from AMQP will arrive
	deliveryChan, err := c.channel.Consume(
		queue.Name, // name
		c.tag,      // consumerTag,
		false,      // autoAck
		false,      // exclusive
		true,       // noLocal
		false,      // noWait
		nil,        // arguments
	)
	if err != nil {
		log.Fatal(err)
	}

	taskChan := make(chan Task)
	resultChan := make(chan Result)

	go backend(resultChan)

	for i := 0; i < NUM_WORKERS; i++ {
		go worker(taskChan, resultChan)
	}

	for dc := range deliveryChan {
		t, err := DecodeAMQPMessageToTask(dc)
		if err != nil {
			log.Error(err)
		} else {
			taskChan <- t
		}
	}
}

func worker(taskChan chan Task, resultChan chan Result) {
	for t := range taskChan {
		// Work goes here
		time.Sleep(1 * time.Second)
		log.Info("Worker received task: ", t)

		r := Result{
			Param3: t.Param1,
			Param4: len(t.Param1),
		}

		resultChan <- r

	}
}

func backend(resultChan chan Result) {

	connection, err := amqp.Dial(rabbitURI)
	if err != nil {
		log.Fatal(err)
	}
	defer connection.Close()

	channel, err := connection.Channel()
	if err != nil {
		log.Fatal(err)
	}

	for r := range resultChan {
		resultBytes, err := json.Marshal(r)
		if err != nil {
			log.Error(err)
			continue
		}

		err = channel.Publish(
			"",                    // Exchange
			RABBITMQ_OUTPUT_QUEUE, // Key (queue)
			false,                 // Mandatory
			false,                 // Immediate
			amqp.Publishing{
				Headers:         amqp.Table{},
				ContentType:     "text/plain",
				ContentEncoding: "",
				DeliveryMode:    0,
				Priority:        5,
				Body:            resultBytes,
			})
		if err != nil {
			log.Error(err)
		}

	}
}

func DecodeAMQPMessageToTask(delivery amqp.Delivery) (Task, error) {
	var task Task
	err := json.Unmarshal(delivery.Body, &task)
	if err != nil {
		return task, err
	}

	// Acknowledge that the task was received
	err = delivery.Ack(false)
	if err != nil {
		return task, err
	}

	return task, nil
}

// Graceful shutdown of a connection to AMQP
func (c *Consumer) Shutdown() error {
	// will close() the deliveries channel
	if err := c.channel.Cancel(c.tag, true); err != nil {
		return err
	}

	if err := c.conn.Close(); err != nil {
		return err
	}

	return nil
}
