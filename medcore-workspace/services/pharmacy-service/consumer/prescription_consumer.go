package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
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

// ===== Compensation Event =====
type StockExhaustedPayload struct {
	AppointmentID  string `json:"appointment_id"`
	PrescriptionID string `json:"prescription_id"`
	DrugCode       string `json:"drug_code"`
	Requested      int    `json:"requested_quantity"`
	Available      int    `json:"available_stock"`
}

type StockExhaustedEvent struct {
	EventID          string                `json:"event_id"`
	EventName        string                `json:"event_name"`
	OccurredAt       string                `json:"occurred_at"`
	Producer         string                `json:"producer"`
	CausationEventID string                `json:"causation_event_id"` // event asal yang memicu kompensasi
	Payload          StockExhaustedPayload `json:"payload"`
}

const (
	exchangeName        = "medcore.clinical.events"
	dlxName             = "medcore.deadletter.exchange"
	dlqName             = "pharmacy.dlq"
	queueName           = "pharmacy.prescription.order.q"
	dbName              = "medcore_pharmacy_db"
	collName            = "processed_events"
	stockExhaustedKey   = "pharmacy.stock.exhausted"
	stockExhaustedEvent = "medcore.event.pharmacy.stock.exhausted"
)

// ===== Simulasi stok gudang (in-memory) =====
var (
	stock   = map[string]int{}
	stockMu sync.Mutex
)

func SetInitialStock(drugCode string, qty int) {
	stockMu.Lock()
	defer stockMu.Unlock()
	stock[drugCode] = qty
	log.Printf("[Pharmacy Stock] Stok awal %s = %d", drugCode, qty)
}

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

	// Channel terpisah untuk menerbitkan event kompensasi
	pubCh, err := conn.Channel()
	if err != nil {
		log.Fatalf("[Pharmacy Consumer] Gagal membuka channel publisher: %v", err)
	}
	defer pubCh.Close()

	// ===== Topologi =====
	if err := ch.ExchangeDeclare(exchangeName, "topic", true, false, false, false, nil); err != nil {
		log.Fatalf("[Pharmacy Consumer] Gagal deklarasi exchange: %v", err)
	}
	if err := ch.ExchangeDeclare(dlxName, "fanout", true, false, false, false, nil); err != nil {
		log.Fatalf("[Pharmacy Consumer] Gagal deklarasi DLX: %v", err)
	}
	if _, err := ch.QueueDeclare(dlqName, true, false, false, false, nil); err != nil {
		log.Fatalf("[Pharmacy Consumer] Gagal deklarasi DLQ: %v", err)
	}
	if err := ch.QueueBind(dlqName, "", dlxName, false, nil); err != nil {
		log.Fatalf("[Pharmacy Consumer] Gagal binding DLQ: %v", err)
	}

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
			ProcessPrescriptionWithDeduplication(ctx, mongoClient, pubCh, d)
			cancel()
		}
	}()

	<-forever
}

func ProcessPrescriptionWithDeduplication(ctx context.Context, mongoClient *mongo.Client, pubCh *amqp.Channel, d amqp.Delivery) {
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

	// ===== Deduplication Guard =====
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

	// ===== Cek ketersediaan stok =====
	stockMu.Lock()
	for _, item := range evt.Payload.Prescription.Items {
		available := stock[item.DrugCode]
		if available < item.Quantity {
			stockMu.Unlock()
			log.Println("---------------------------------------------------------")
			log.Printf("[Pharmacy Stock] STOK TIDAK CUKUP! %s: diminta %d, tersedia %d", item.DrugCode, item.Quantity, available)

			if err := publishStockExhausted(ctx, pubCh, evt, item.DrugCode, item.Quantity, available); err != nil {
				log.Printf("[Error] Gagal menerbitkan event kompensasi: %v. Requeue pesan.", err)
				d.Nack(false, true)
				return
			}

			recordProcessed(ctx, eventColl, evt.EventID, "STOCK_EXHAUSTED")
			d.Ack(false)
			log.Println("---------------------------------------------------------")
			return
		}
	}

	// ===== Logika bisnis: potong stok =====
	log.Println("---------------------------------------------------------")
	log.Printf("[Business Logic] Memproses stok resep untuk Event ID: %s", evt.EventID)
	log.Printf("  Prescription ID : %s", evt.Payload.Prescription.PrescriptionID)
	for _, item := range evt.Payload.Prescription.Items {
		stock[item.DrugCode] -= item.Quantity
		log.Printf("  -> Potong stok: %s sebanyak %d (sisa %d)", item.DrugCode, item.Quantity, stock[item.DrugCode])
	}
	stockMu.Unlock()

	if !recordProcessed(ctx, eventColl, evt.EventID, "SUCCESS") {
		d.Nack(false, true)
		return
	}

	d.Ack(false)
	log.Printf("[Pharmacy Consumer] Transaksi selesai sempurna dan Event %s dicatat.", evt.EventID)
	log.Println("---------------------------------------------------------")
}

func recordProcessed(ctx context.Context, coll *mongo.Collection, eventID, status string) bool {
	_, err := coll.InsertOne(ctx, ProcessedEventRecord{
		EventID:     eventID,
		ProcessedAt: time.Now(),
		Status:      status,
	})
	if err != nil && !mongo.IsDuplicateKeyError(err) {
		log.Printf("[Error] Gagal mencatat deduplikasi: %v", err)
		return false
	}
	return true
}

func publishStockExhausted(ctx context.Context, ch *amqp.Channel, src EventEnvelope, drugCode string, requested, available int) error {
	compEvt := StockExhaustedEvent{
		EventID:          fmt.Sprintf("evt-%s", uuid.New().String()),
		EventName:        stockExhaustedEvent,
		OccurredAt:       time.Now().UTC().Format(time.RFC3339),
		Producer:         "pharmacy-service",
		CausationEventID: src.EventID,
		Payload: StockExhaustedPayload{
			AppointmentID:  src.Payload.AppointmentID,
			PrescriptionID: src.Payload.Prescription.PrescriptionID,
			DrugCode:       drugCode,
			Requested:      requested,
			Available:      available,
		},
	}

	body, err := json.Marshal(compEvt)
	if err != nil {
		return err
	}

	err = ch.PublishWithContext(ctx, exchangeName, stockExhaustedKey, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "application/json",
		MessageId:    compEvt.EventID,
		Timestamp:    time.Now(),
		Type:         stockExhaustedEvent,
		Body:         body,
	})
	if err != nil {
		return err
	}

	log.Printf("[Compensation] Menerbitkan %s (ID: %s) untuk Appointment %s", stockExhaustedEvent, compEvt.EventID, src.Payload.AppointmentID)
	return nil
}