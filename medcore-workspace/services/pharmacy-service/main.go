package main

import (
	"context"
	"flag"
	"log"
	"time"

	"pharmacy-service/consumer"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	// -stock=0 -> simulasi stok MED-AMX-500 kosong
	initialStock := flag.Int("stock", 100, "stok awal MED-AMX-500")
	flag.Parse()

	amqpURI := "amqp://admin_medcore:SecuredPassRabbit2026!@localhost:5672/%2Fmedcore_vhost"
	mongoURI := "mongodb://localhost:27017"

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Fatalf("[Pharmacy Service] Gagal koneksi MongoDB: %v", err)
	}
	defer mongoClient.Disconnect(context.Background())

	if err := mongoClient.Ping(ctx, nil); err != nil {
		log.Fatalf("[Pharmacy Service] MongoDB tidak merespons: %v", err)
	}
	log.Println("[Pharmacy Service] Terhubung ke MongoDB.")

	consumer.SetInitialStock("MED-AMX-500", *initialStock)

	log.Println("[Pharmacy Service] Memulai consumer resep...")
	consumer.StartPrescriptionConsumer(amqpURI, mongoClient)
}