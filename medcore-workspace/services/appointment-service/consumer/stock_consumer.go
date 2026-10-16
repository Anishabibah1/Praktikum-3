package consumer

import (
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type StockExhaustedEvent struct {
	EventID          string `json:"event_id"`
	EventName        string `json:"event_name"`
	CausationEventID string `json:"causation_event_id"`
	Payload          struct {
		AppointmentID  string `json:"appointment_id"`
		PrescriptionID string `json:"prescription_id"`
		DrugCode       string `json:"drug_code"`
		Requested      int    `json:"requested_quantity"`
		Available      int    `json:"available_stock"`
	} `json:"payload"`
}

func StartStockExhaustedConsumer(amqpURI string) {
	conn, err := amqp.Dial(amqpURI)
	if err != nil {
		log.Fatalf("[Appointment Listener] Gagal koneksi broker: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("[Appointment Listener] Gagal membuka channel: %v", err)
	}
	defer ch.Close()

	if err := ch.ExchangeDeclare("medcore.clinical.events", "topic", true, false, false, false, nil); err != nil {
		log.Fatalf("[Appointment Listener] Gagal deklarasi exchange: %v", err)
	}

	q, err := ch.QueueDeclare("appointment.stock.exhausted.q", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("[Appointment Listener] Gagal deklarasi queue: %v", err)
	}

	if err := ch.QueueBind(q.Name, "pharmacy.stock.exhausted", "medcore.clinical.events", false, nil); err != nil {
		log.Fatalf("[Appointment Listener] Gagal binding queue: %v", err)
	}

	if err := ch.Qos(1, 0, false); err != nil {
		log.Fatalf("[Appointment Listener] Gagal set QoS: %v", err)
	}

	msgs, err := ch.Consume(q.Name, "appointment-stock-listener", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("[Appointment Listener] Gagal subscribe: %v", err)
	}

	log.Printf("[Appointment Listener] Mendengarkan event kegagalan stok pada antrean: %s", q.Name)

	forever := make(chan bool)
	go func() {
		for d := range msgs {
			var evt StockExhaustedEvent
			if err := json.Unmarshal(d.Body, &evt); err != nil {
				log.Printf("[Appointment Listener] Format tidak valid, NACK: %v", err)
				d.Nack(false, false)
				continue
			}

			log.Println("=========================================================")
			log.Printf("[Appointment Listener] Menerima %s", evt.EventName)
			log.Printf("  Event ID        : %s", evt.EventID)
			log.Printf("  Dipicu oleh     : %s", evt.CausationEventID)
			log.Printf("  Appointment ID  : %s", evt.Payload.AppointmentID)
			log.Printf("  Obat            : %s (diminta %d, tersedia %d)", evt.Payload.DrugCode, evt.Payload.Requested, evt.Payload.Available)

			// Simulasi update status appointment
			// (pada sistem penuh: UPDATE appointments SET status = ... di medcore-appointment-db)
			log.Printf("[Appointment Listener] Status appointment %s diubah: COMPLETED -> PENDING_MEDICATION_SUBSTITUTION", evt.Payload.AppointmentID)
			log.Println("=========================================================")

			d.Ack(false)
		}
	}()

	<-forever
}