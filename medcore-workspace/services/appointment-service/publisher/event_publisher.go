package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type EventPublisher struct {
	conn    *amqp.Connection
	channel *amqp.Channel
}

type PrescriptionItem struct {
	DrugCode string `json:"drug_code"`
	Quantity int    `json:"quantity"`
	Dosage   string `json:"dosage"`
}

type PrescriptionData struct {
	PrescriptionID string             `json:"prescription_id"`
	Items          []PrescriptionItem `json:"items"`
}

type BillingData struct {
	ConsultationFee float64 `json:"consultation_fee"`
	TriageUrgency   string  `json:"triage_urgency"`
}

type ConsultationPayload struct {
	AppointmentID      string           `json:"appointment_id"`
	PatientID          string           `json:"patient_id"`
	DoctorID           string           `json:"doctor_id"`
	ConsultationStatus string           `json:"consultation_status"`
	Prescription       PrescriptionData `json:"prescription"`
	BillingSummary     BillingData      `json:"billing_summary"`
}

type ConsultationCompletedEvent struct {
	EventID    string              `json:"event_id"`
	EventName  string              `json:"event_name"`
	OccurredAt string              `json:"occurred_at"`
	Producer   string              `json:"producer"`
	Payload    ConsultationPayload `json:"payload"`
}

func NewEventPublisher(amqpURI string) (*EventPublisher, error) {
	conn, err := amqp.Dial(amqpURI)
	if err != nil {
		return nil, fmt.Errorf("gagal koneksi ke RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("gagal membuka channel AMQP: %w", err)
	}

	// Deklarasikan Topic Exchange utama
	err = ch.ExchangeDeclare(
		"medcore.clinical.events", // Nama Exchange
		"topic",                   // Tipe Exchange
		true,                      // Durable
		false,                     // Auto-deleted
		false,                     // Internal
		false,                     // No-wait
		nil,                       // Arguments
	)
	if err != nil {
		return nil, fmt.Errorf("gagal mendeklarasikan exchange: %w", err)
	}

	return &EventPublisher{conn: conn, channel: ch}, nil
}

func (p *EventPublisher) PublishConsultationCompleted(ctx context.Context, event ConsultationCompletedEvent) error {
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("gagal serialisasi JSON event: %w", err)
	}

	routingKey := "clinical.consultation.completed"

	err = p.channel.PublishWithContext(
		ctx,
		"medcore.clinical.events", // Exchange tujuan
		routingKey,                // Routing Key
		true,                      // Mandatory
		false,                     // Immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent, // Tulis ke disk
			ContentType:  "application/json",
			MessageId:    event.EventID,
			Timestamp:    time.Now(),
			Type:         event.EventName,
			Body:         body,
		},
	)
	if err != nil {
		return fmt.Errorf("gagal menerbitkan pesan: %w", err)
	}

	log.Printf("[AMQP Publisher] Berhasil menerbitkan event ID: %s dengan Routing Key: %s", event.EventID, routingKey)
	return nil
}

func (p *EventPublisher) Close() {
	if p.channel != nil {
		p.channel.Close()
	}
	if p.conn != nil {
		p.conn.Close()
	}
}