package io.sandonjacobs.ato;

import io.sandonjacobs.ato.agent.TicketRoutingAgent;
import io.sandonjacobs.ato.avro.SupportTicket;
import io.sandonjacobs.ato.avro.TicketRouting;
import io.sandonjacobs.ato.model.RoutingDecision;
import io.sandonjacobs.ato.model.Ticket;
import org.apache.flink.agents.api.AgentsExecutionEnvironment;
import org.apache.flink.agents.api.agents.Agent;
import org.apache.flink.agents.api.agents.AgentExecutionOptions;
import org.apache.flink.agents.api.resource.ResourceDescriptor;
import org.apache.flink.agents.api.resource.ResourceName;
import org.apache.flink.agents.api.resource.ResourceType;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.nio.charset.StandardCharsets;

/**
 * Kafka (Avro) ──► Flink ──► TicketRoutingAgent (LLM on Ollama) ──► Kafka (Avro).
 *
 * <p>Runs on a local MiniCluster with the Flink web UI at http://localhost:8088.
 */
public class TicketRoutingJob {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, AppConfig.FLINK_UI_PORT);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);
        env.enableCheckpointing(10_000);

        // ---- Source: Avro tickets from Kafka, schema from Schema Registry ----
        KafkaSource<SupportTicket> source =
                KafkaSource.<SupportTicket>builder()
                        .setBootstrapServers(AppConfig.BOOTSTRAP_SERVERS)
                        .setTopics(AppConfig.TICKETS_TOPIC)
                        .setGroupId("ticket-router")
                        .setStartingOffsets(OffsetsInitializer.latest())
                        .setValueOnlyDeserializer(
                                ConfluentRegistryAvroDeserializationSchema.forSpecific(
                                        SupportTicket.class, AppConfig.SCHEMA_REGISTRY_URL))
                        .build();

        DataStream<Ticket> tickets =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "support-tickets")
                        .map(Ticket::fromAvro)
                        .name("avro-to-ticket");

        // ---- The agent: an LLM-backed operator inside the Flink job ----
        AgentsExecutionEnvironment agentsEnv = AgentsExecutionEnvironment.getExecutionEnvironment(env);
        agentsEnv.getConfig().set(AgentExecutionOptions.NUM_ASYNC_THREADS, 2);
        // LLMs occasionally return unparseable output: retry the chat call instead of failing the job.
        agentsEnv.getConfig().set(AgentExecutionOptions.ERROR_HANDLING_STRATEGY, Agent.ErrorHandlingStrategy.RETRY);
        agentsEnv.getConfig().set(AgentExecutionOptions.MAX_RETRIES, 3);
        agentsEnv.addResource(
                TicketRoutingAgent.OLLAMA_CONNECTION,
                ResourceType.CHAT_MODEL_CONNECTION,
                ResourceDescriptor.Builder.newBuilder(ResourceName.ChatModel.OLLAMA_CONNECTION)
                        .addInitialArgument("endpoint", AppConfig.OLLAMA_ENDPOINT)
                        .addInitialArgument("requestTimeout", 180)
                        .build());

        DataStream<Object> agentOutput =
                agentsEnv
                        .fromDataStream(tickets, Ticket::getTicketId)
                        .apply(new TicketRoutingAgent())
                        .toDataStream();

        DataStream<TicketRouting> routed =
                agentOutput
                        .map(o -> ((RoutingDecision) o).toAvro())
                        .returns(TicketRouting.class)
                        .name("decision-to-avro");

        // ---- Sink: Avro routing decisions back to Kafka, keyed by ticket id ----
        KafkaSink<TicketRouting> sink =
                KafkaSink.<TicketRouting>builder()
                        .setBootstrapServers(AppConfig.BOOTSTRAP_SERVERS)
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.<TicketRouting>builder()
                                        .setTopic(AppConfig.ROUTED_TOPIC)
                                        .setKeySerializationSchema(
                                                (SerializationSchema<TicketRouting>)
                                                        r -> r.getTicketId().getBytes(StandardCharsets.UTF_8))
                                        .setValueSerializationSchema(
                                                ConfluentRegistryAvroSerializationSchema.forSpecific(
                                                        TicketRouting.class,
                                                        AppConfig.ROUTED_TOPIC + "-value",
                                                        AppConfig.SCHEMA_REGISTRY_URL))
                                        .build())
                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .build();

        routed.sinkTo(sink).name("routed-tickets");
        agentOutput.print().name("console");

        agentsEnv.execute("Support Ticket Routing Agent");
    }
}
