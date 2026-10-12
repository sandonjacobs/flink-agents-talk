package io.sandonjacobs.ato.tools;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.sandonjacobs.ato.AppConfig;
import io.sandonjacobs.ato.avro.TicketRouting;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/** Tails the routed-tickets topic and prints each decision. */
public class RoutingConsumer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "routing-viewer-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, args.length > 0 ? args[0] : "latest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        props.put("schema.registry.url", AppConfig.SCHEMA_REGISTRY_URL);

        try (KafkaConsumer<String, TicketRouting> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(AppConfig.ROUTED_TOPIC));
            System.out.println("Watching " + AppConfig.ROUTED_TOPIC + " ...");
            while (true) {
                for (ConsumerRecord<String, TicketRouting> rec : consumer.poll(Duration.ofSeconds(1))) {
                    TicketRouting r = rec.value();
                    System.out.printf(
                            "%s  %-8s %-18s %s%n      ↳ %s%n",
                            r.getEscalated() ? "📟" : "✅",
                            r.getTicketId(),
                            r.getTeam() + " [" + r.getPriority() + "]",
                            r.getSubject(),
                            r.getReasoning());
                }
            }
        }
    }
}
