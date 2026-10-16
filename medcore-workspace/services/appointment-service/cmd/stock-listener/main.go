package main

import (
	"log"

	"appointment-service/consumer"
)

func main() {
	amqpURI := "amqp://admin_medcore:SecuredPassRabbit2026!@localhost:5672/%2Fmedcore_vhost"

	log.Println("[Appointment Service] Memulai listener event kompensasi stok...")
	consumer.StartStockExhaustedConsumer(amqpURI)
}