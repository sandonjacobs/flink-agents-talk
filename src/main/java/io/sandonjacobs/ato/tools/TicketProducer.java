package io.sandonjacobs.ato.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.sandonjacobs.ato.AppConfig;
import io.sandonjacobs.ato.avro.Channel;
import io.sandonjacobs.ato.avro.SupportTicket;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Produces Avro support tickets to Kafka.
 *
 * <ul>
 *   <li>No args: sends the canned tickets in {@code sample-tickets.json}, a few seconds apart.
 *   <li>{@code --interactive}: type a ticket on stdin ({@code subject | description}) — great for
 *       letting the audience suggest one.
 * </ul>
 */
public class TicketProducer {

    private static final AtomicInteger SEQ = new AtomicInteger(1000 + (int) (System.currentTimeMillis() % 9000));

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put("schema.registry.url", AppConfig.SCHEMA_REGISTRY_URL);

        boolean interactive = args.length > 0 && args[0].equals("--interactive");

        try (KafkaProducer<String, SupportTicket> producer = new KafkaProducer<>(props)) {
            if (interactive) {
                runInteractive(producer);
            } else {
                runSamples(producer, args.length > 0 ? Long.parseLong(args[0]) : 3000L);
            }
        }
    }

    private static void runSamples(KafkaProducer<String, SupportTicket> producer, long delayMs)
            throws Exception {
        try (InputStream in = TicketProducer.class.getClassLoader().getResourceAsStream("sample-tickets.json")) {
            for (JsonNode node : new ObjectMapper().readTree(in)) {
                send(
                        producer,
                        node.get("customer_id").asText(),
                        Channel.valueOf(node.get("channel").asText()),
                        node.get("subject").asText(),
                        node.get("description").asText());
                Thread.sleep(delayMs);
            }
        }
    }

    private static void runInteractive(KafkaProducer<String, SupportTicket> producer) throws Exception {
        System.out.println("Enter tickets as:  subject | description   (Ctrl-D to quit)");
        BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
        System.out.print("> ");
        for (String line; (line = stdin.readLine()) != null; System.out.print("> ")) {
            if (line.isBlank()) {
                continue;
            }
            String[] parts = line.split("\\|", 2);
            String subject = parts[0].trim();
            String description = parts.length > 1 ? parts[1].trim() : subject;
            send(producer, "live-audience", Channel.WEB, subject, description);
        }
    }

    private static void send(
            KafkaProducer<String, SupportTicket> producer,
            String customerId,
            Channel channel,
            String subject,
            String description)
            throws Exception {
        String ticketId = "T-" + SEQ.getAndIncrement();
        SupportTicket ticket =
                SupportTicket.newBuilder()
                        .setTicketId(ticketId)
                        .setCustomerId(customerId)
                        .setChannel(channel)
                        .setSubject(subject)
                        .setDescription(description)
                        .setCreatedAt(Instant.now())
                        .build();
        producer.send(new ProducerRecord<>(AppConfig.TICKETS_TOPIC, ticketId, ticket)).get();
        System.out.printf("🎫  %s  %-40s (%s)%n", ticketId, subject, customerId);
    }
}
