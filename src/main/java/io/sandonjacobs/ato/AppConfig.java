package io.sandonjacobs.ato;

/** Demo configuration, overridable with environment variables. */
public final class AppConfig {

    public static final String TICKETS_TOPIC = env("TICKETS_TOPIC", "support.tickets");
    public static final String ROUTED_TOPIC = env("ROUTED_TOPIC", "support.tickets.routed");

    public static final String BOOTSTRAP_SERVERS = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    public static final String SCHEMA_REGISTRY_URL = env("SCHEMA_REGISTRY_URL", "http://localhost:8081");

    public static final String OLLAMA_ENDPOINT = env("OLLAMA_ENDPOINT", "http://localhost:11434");
    public static final String OLLAMA_MODEL = env("OLLAMA_MODEL", "qwen2.5:7b");

    /** Port for the local Flink web UI (8081 is taken by Schema Registry). */
    public static final int FLINK_UI_PORT = Integer.parseInt(env("FLINK_UI_PORT", "8088"));

    private AppConfig() {}

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
