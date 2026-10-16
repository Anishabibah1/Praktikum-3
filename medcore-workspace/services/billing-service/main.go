package main

import (
	"log"

	"billing-service/consumer"
)

func main() {
	amqpURI := "amqp://admin_medcore:SecuredPassRabbit2026!@localhost:5672/%2Fmedcore_vhost"

	log.Println("[Billing Service] Memulai consumer tagihan...")
	consumer.StartBillingConsumer(amqpURI)
}