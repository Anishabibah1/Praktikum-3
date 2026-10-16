package consumer

import (
	"encoding/json"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type BillingEnvelope struct {
	EventID string `json:"event_id"`
	Payload struct {
		AppointmentID  string `json:"appointment_id"`
		PatientID      string `json:"patient_id"`
		BillingSummary struct {
			ConsultationFee float64 `json:"consultation_fee"`
			TriageUrgency   string  `json:"triage_urgency"`
		} `json:"billing_summary"`
	} `json:"payload"`
}

func StartBillingConsumer(amqpURI string) {
	conn, err := amqp.Dial(amqpURI)
	if err != nil {
		log.Fatalf("[Billing Consumer] Gagal koneksi broker: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("[Billing Consumer] Gagal membuka channel: %v", err)
	}
	defer ch.Close()

	q, err := ch.QueueDeclare("billing.invoice.q", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("[Billing Consumer] Gagal deklarasi queue: %v", err)
	}

	// Sesuai diagram topologi modul: binding "clinical.*.completed"
	err = ch.QueueBind(q.Name, "clinical.*.completed", "medcore.clinical.events", false, nil)
	if err != nil {
		log.Fatalf("[Billing Consumer] Gagal binding queue: %v", err)
	}

	if err = ch.Qos(1, 0, false); err != nil {
		log.Fatalf("[Billing Consumer] Gagal set QoS: %v", err)
	}

	msgs, err := ch.Consume(q.Name, "billing-worker", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("[Billing Consumer] Gagal subscribe: %v", err)
	}

	log.Printf("[Billing Consumer] Menunggu event pada antrean: %s", q.Name)

	forever := make(chan bool)
	go func() {
		for d := range msgs {
			var evt BillingEnvelope
			if err := json.Unmarshal(d.Body, &evt); err != nil {
				log.Printf("[Billing Consumer] Format tidak valid, NACK: %v", err)
				d.Nack(false, false)
				continue
			}

			log.Println("---------------------------------------------------------")
			log.Printf("[Billing Consumer] Membuat draf tagihan untuk Event ID: %s", evt.EventID)
			log.Printf("  Pasien     : %s", evt.Payload.PatientID)
			log.Printf("  Biaya      : Rp %.0f", evt.Payload.BillingSummary.ConsultationFee)
			log.Printf("  Triage     : %s", evt.Payload.BillingSummary.TriageUrgency)

			// Simulasi proses lambat agar status Unacked terlihat di dashboard
			log.Println("[Billing Consumer] Memproses tagihan (10 detik)... cek dashboard sekarang!")
			time.Sleep(10 * time.Second)

			d.Ack(false)
			log.Println("[Billing Consumer] Tagihan tercatat, pesan di-ACK.")
			log.Println("---------------------------------------------------------")
		}
	}()

	<-forever
}