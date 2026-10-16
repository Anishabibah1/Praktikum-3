package consumer

import (
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
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

const (
	exchangeName = "medcore.clinical.events"
	dlxName      = "medcore.deadletter.exchange"
	dlqName      = "pharmacy.dlq"
	queueName    = "pharmacy.prescription.order.q"
)

func StartPrescriptionConsumer(amqpURI string) {
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

	// ===== Topologi DLX (pengganti skrip setup_rabbitmq_topology.sh) =====
	if err := ch.ExchangeDeclare(dlxName, "fanout", true, false, false, false, nil); err != nil {
		log.Fatalf("[Pharmacy Consumer] Gagal deklarasi DLX: %v", err)
	}
	if _, err := ch.QueueDeclare(dlqName, true, false, false, false, nil); err != nil {
		log.Fatalf("[Pharmacy Consumer] Gagal deklarasi DLQ: %v", err)
	}
	if err := ch.QueueBind(dlqName, "", dlxName, false, nil); err != nil {
		log.Fatalf("[Pharmacy Consumer] Gagal binding DLQ: %v", err)
	}

	// ===== Queue utama dengan Dead Letter Exchange =====
	args := amqp.Table{
		"x-dead-letter-exchange": dlxName, // Pesan yang di-NACK (requeue=false) dialihkan ke DLX
	}
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

	msgs, err := ch.Consume(q.Name, "pharmacy-worker", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("[Pharmacy Consumer] Gagal subscribe: %v", err)
	}

	log.Printf("[Pharmacy Consumer] Menunggu pesan resep pada antrean: %s", q.Name)

	forever := make(chan bool)
	go func() {
		for d := range msgs {
			var evt EventEnvelope
			if err := json.Unmarshal(d.Body, &evt); err != nil {
				log.Printf("[Pharmacy Consumer] Format JSON rusak, NACK ke DLQ: %v", err)
				d.Nack(false, false)
				continue
			}

			// ===== Validasi Poison Message =====
			// Dicek dulu SEMUA item, baru diputuskan ACK atau NACK (hanya salah satu!)
			poison := false
			for _, item := range evt.Payload.Prescription.Items {
				if item.Quantity <= 0 {
					log.Printf("[Pharmacy Consumer] Terdeteksi Poison Message! Kuantitas tidak valid: %d. Mengirim NACK ke DLQ.", item.Quantity)
					poison = true
					break
				}
			}
			if poison {
				d.Nack(false, false) // requeue = false -> otomatis dialihkan ke pharmacy.dlq
				continue
			}

			log.Println("---------------------------------------------------------")
			log.Printf("[Pharmacy Consumer] Menerima Resep Baru!")
			log.Printf("  Event ID        : %s", evt.EventID)
			log.Printf("  Appointment ID  : %s", evt.Payload.AppointmentID)
			log.Printf("  Prescription ID : %s", evt.Payload.Prescription.PrescriptionID)
			for _, item := range evt.Payload.Prescription.Items {
				log.Printf("  -> Item: %s | Qty: %d", item.DrugCode, item.Quantity)
			}
			log.Println("---------------------------------------------------------")

			d.Ack(false)
			log.Println("[Pharmacy Consumer] Pesan berhasil di-ACK.")
		}
	}()

	<-forever
}