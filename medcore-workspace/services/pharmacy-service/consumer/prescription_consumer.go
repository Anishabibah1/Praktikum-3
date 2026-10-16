package consumer

import (
	"context"
	"encoding/json"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type EventEnvelope struct {
	EventID string `json:"event_id"`
	Payload struct {
		AppointmentID string `json:"appointment_id"`
		Prescription  struct {
			PrescriptionID string `json:"prescription_id"`
			Items          []struct {
				DrugCode string `json:"drug_code"`
				Quantity int    `json:"quantity"`
			} `json:"items"`
		} `json:"prescription"`
	} `json:"payload"`
}

type ProcessedEventRecord struct {
	EventID     string    `bson:"event_id"`
	ProcessedAt time.Time `bson:"processed_at"`
	Status      string    `bson:"status"`
}

const (
	exchangeName = "medcore.clinical.events"
	dlxName      = "medcore.deadletter.exchange"
	dlqName      = "pharmacy.dlq"
	queueName    = "pharmacy.prescription.order.q"
	dbName       = "medcore_pharmacy_db"
	collName     = "processed_events"
)

func StartPrescriptionConsumer(amqpURI string, mongoClient *mongo.Client) {
	conn, err := amqp.Dial(amqpURI)
	if err != nil {
		log.Fatalf("[Pharmacy Consumer] Gagal koneksi broker: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("[Pharmacy Consumer] Gagal membuka channel: %v", err)
	}
	defer ch.Close()

	// ===== Topologi DLX =====
	if err := ch.ExchangeDeclare(dlxName, "fanout", true, false, false, false, nil); err != nil {
		log.Fatalf("[Pharmacy Consumer] Gagal deklarasi DLX: %v", err)
	}
	if _, err := ch.QueueDeclare(dlqName, true, false, false, false, nil); err != nil {
		log.Fatalf("[Pharmacy Consumer] Gagal deklarasi DLQ: %v", err)
	}
	if err := ch.QueueBind(dlqName, "", dlxName, false, nil); err != nil {
		log.Fatalf("[Pharmacy Consumer] Gagal binding DLQ: %v", err)
	}

	// ===== Queue utama dengan DLX =====
	args := amqp.Table{"x-dead-letter-exchange": dlxName}
	q, err := ch.QueueDeclare(queueName, true, false, false, false, args)
	if err != nil {
		log.Fatalf("[Pharmacy Consumer] Gagal deklarasi queue: %v", err)
	}
	if err := ch.QueueBind(q.Name, "clinical.consultation.*", exchangeName, false, nil); err != nil {
		log.Fatalf("[Pharmacy Consumer] Gagal binding queue: %v", err)
	}
	if err := ch.Qos(1, 0, false); err != nil {
		log.Fatalf("[Pharmacy Consumer] Gagal set QoS: %v", err)
	}

	// ===== Unique index pada event_id =====
	// Jaminan tambahan: MongoDB menolak event_id ganda walau dua pesan datang bersamaan
	coll := mongoClient.Database(dbName).Collection(collName)
	_, err = coll.Indexes().CreateOne(context.Background(), mongo.IndexModel{
		Keys:    bson.D{{Key: "event_id", Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	if err != nil {
		log.Fatalf("[Pharmacy Consumer] Gagal membuat unique index: %v", err)
	}

	msgs, err := ch.Consume(q.Name, "pharmacy-worker", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("[Pharmacy Consumer] Gagal subscribe: %v", err)
	}

	log.Printf("[Pharmacy Consumer] Menunggu pesan resep pada antrean: %s (dengan Deduplication Guard)", q.Name)

	forever := make(chan bool)
	go func() {
		for d := range msgs {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			ProcessPrescriptionWithDeduplication(ctx, mongoClient, d)
			cancel()
		}
	}()

	<-forever
}

func ProcessPrescriptionWithDeduplication(ctx context.Context, mongoClient *mongo.Client, d amqp.Delivery) {
	var evt EventEnvelope
	if err := json.Unmarshal(d.Body, &evt); err != nil {
		log.Printf("[Pharmacy Consumer] Format JSON rusak, NACK ke DLQ: %v", err)
		d.Nack(false, false)
		return
	}

	// ===== Validasi Poison Message =====
	for _, item := range evt.Payload.Prescription.Items {
		if item.Quantity <= 0 {
			log.Printf("[Pharmacy Consumer] Terdeteksi Poison Message! Kuantitas tidak valid: %d. Mengirim NACK ke DLQ.", item.Quantity)
			d.Nack(false, false)
			return
		}
	}

	eventColl := mongoClient.Database(dbName).Collection(collName)

	// 1. Cek apakah Event ID sudah pernah diproses
	var existing ProcessedEventRecord
	err := eventColl.FindOne(ctx, bson.M{"event_id": evt.EventID}).Decode(&existing)
	if err == nil {
		log.Printf("[Deduplication Guard] Event %s terdeteksi duplikat! Melewati eksekusi bisnis dan langsung mengirim ACK.", evt.EventID)
		d.Ack(false)
		return
	}
	if err != mongo.ErrNoDocuments {
		log.Printf("[Error] Gagal cek MongoDB: %v. Requeue pesan.", err)
		d.Nack(false, true)
		return
	}

	// 2. Eksekusi logika bisnis (simulasi pemotongan stok)
	log.Println("---------------------------------------------------------")
	log.Printf("[Business Logic] Memproses stok resep untuk Event ID: %s", evt.EventID)
	log.Printf("  Prescription ID : %s", evt.Payload.Prescription.PrescriptionID)
	for _, item := range evt.Payload.Prescription.Items {
		log.Printf("  -> Potong stok: %s sebanyak %d", item.DrugCode, item.Quantity)
	}

	// 3. Catat Event ID ke riwayat proses
	_, err = eventColl.InsertOne(ctx, ProcessedEventRecord{
		EventID:     evt.EventID,
		ProcessedAt: time.Now(),
		Status:      "SUCCESS",
	})
	if mongo.IsDuplicateKeyError(err) {
		log.Printf("[Deduplication Guard] Event %s sudah tercatat (race condition), ACK.", evt.EventID)
		d.Ack(false)
		return
	}
	if err != nil {
		log.Printf("[Error] Gagal mencatat deduplikasi: %v. Requeue pesan.", err)
		d.Nack(false, true)
		return
	}

	d.Ack(false)
	log.Printf("[Pharmacy Consumer] Transaksi selesai sempurna dan Event %s dicatat.", evt.EventID)
	log.Println("---------------------------------------------------------")
}