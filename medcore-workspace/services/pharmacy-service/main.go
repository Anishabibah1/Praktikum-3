package main

import (
	"log"

	"pharmacy-service/consumer"
)

func main() {
	amqpURI := "amqp://admin_medcore:SecuredPassRabbit2026!@localhost:5672/%2Fmedcore_vhost"

	log.Println("[Pharmacy Service] Memulai consumer resep...")
	consumer.StartPrescriptionConsumer(amqpURI)
}