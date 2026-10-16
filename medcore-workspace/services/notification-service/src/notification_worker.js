const amqp = require('amqplib');

// %2F = garis miring "/" pada nama vhost "/medcore_vhost"
const AMQP_URI = 'amqp://admin_medcore:SecuredPassRabbit2026!@localhost:5672/%2Fmedcore_vhost';
const EXCHANGE_NAME = 'medcore.clinical.events';
const QUEUE_NAME = 'notification.patient.alert.q';
const ROUTING_KEY_PATTERN = 'clinical.#'; // Mendengarkan seluruh event klinis

async function startNotificationWorker() {
  try {
    const connection = await amqp.connect(AMQP_URI);
    const channel = await connection.createChannel();

    await channel.assertExchange(EXCHANGE_NAME, 'topic', { durable: true });
    const q = await channel.assertQueue(QUEUE_NAME, { durable: true });
    await channel.bindQueue(q.queue, EXCHANGE_NAME, ROUTING_KEY_PATTERN);

    await channel.prefetch(1);
    console.log(`[Notification Worker] Berjalan pada antrean: ${q.queue}`);

    channel.consume(q.queue, (msg) => {
      if (msg !== null) {
        const eventData = JSON.parse(msg.content.toString());
        console.log(`\n[SMS/WhatsApp Gateway] Mengirim Notifikasi ke Pasien: ${eventData.payload.patient_id}`);
        console.log(`Pesan: "Sesi konsultasi Anda (${eventData.payload.appointment_id}) telah selesai. Resep sedang disiapkan oleh Farmasi."`);

        channel.ack(msg);
        console.log(`[Notification Worker] Event ${eventData.event_id} di-ACK.`);
      }
    }, { noAck: false });

  } catch (error) {
    console.error('[Notification Worker] Error:', error);
  }
}

startNotificationWorker();