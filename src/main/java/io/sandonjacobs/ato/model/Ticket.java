package io.sandonjacobs.ato.model;

import io.sandonjacobs.ato.avro.SupportTicket;

/**
 * The ticket as the agent sees it. Avro stays at the Kafka edges; inside the agent we use a plain
 * POJO so Flink and the agent runtime can serialize it without surprises.
 */
public class Ticket {

    private String ticketId;
    private String customerId;
    private String channel;
    private String subject;
    private String description;

    public Ticket() {}

    public static Ticket fromAvro(SupportTicket avro) {
        Ticket t = new Ticket();
        t.ticketId = avro.getTicketId();
        t.customerId = avro.getCustomerId();
        t.channel = avro.getChannel().name();
        t.subject = avro.getSubject();
        t.description = avro.getDescription();
        return t;
    }

    public String getTicketId() { return ticketId; }
    public void setTicketId(String ticketId) { this.ticketId = ticketId; }
    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }
    public String getChannel() { return channel; }
    public void setChannel(String channel) { this.channel = channel; }
    public String getSubject() { return subject; }
    public void setSubject(String subject) { this.subject = subject; }
    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }

    @Override
    public String toString() {
        return "Ticket{" + ticketId + ", '" + subject + "'}";
    }
}
