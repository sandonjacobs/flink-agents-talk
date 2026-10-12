package io.sandonjacobs.ato.model;

import io.sandonjacobs.ato.avro.Priority;
import io.sandonjacobs.ato.avro.TicketRouting;

import java.time.Instant;

/** What the agent emits downstream; converted to Avro {@link TicketRouting} at the sink. */
public class RoutingDecision {

    private String ticketId;
    private String subject;
    private String team;
    private String priority;
    private String reasoning;
    private boolean escalated;
    private String model;
    private long routedAt;

    public RoutingDecision() {}

    public RoutingDecision(
            String ticketId,
            String subject,
            String team,
            String priority,
            String reasoning,
            boolean escalated,
            String model) {
        this.ticketId = ticketId;
        this.subject = subject;
        this.team = team;
        this.priority = priority;
        this.reasoning = reasoning;
        this.escalated = escalated;
        this.model = model;
        this.routedAt = System.currentTimeMillis();
    }

    public TicketRouting toAvro() {
        return TicketRouting.newBuilder()
                .setTicketId(ticketId)
                .setSubject(subject)
                .setTeam(team)
                .setPriority(Priority.valueOf(priority))
                .setReasoning(reasoning)
                .setEscalated(escalated)
                .setModel(model)
                .setRoutedAt(Instant.ofEpochMilli(routedAt))
                .build();
    }

    public String getTicketId() { return ticketId; }
    public void setTicketId(String ticketId) { this.ticketId = ticketId; }
    public String getSubject() { return subject; }
    public void setSubject(String subject) { this.subject = subject; }
    public String getTeam() { return team; }
    public void setTeam(String team) { this.team = team; }
    public String getPriority() { return priority; }
    public void setPriority(String priority) { this.priority = priority; }
    public String getReasoning() { return reasoning; }
    public void setReasoning(String reasoning) { this.reasoning = reasoning; }
    public boolean isEscalated() { return escalated; }
    public void setEscalated(boolean escalated) { this.escalated = escalated; }
    public String getModel() { return model; }
    public void setModel(String model) { this.model = model; }
    public long getRoutedAt() { return routedAt; }
    public void setRoutedAt(long routedAt) { this.routedAt = routedAt; }

    @Override
    public String toString() {
        return String.format(
                "[%s] %s -> %s (%s%s): %s",
                ticketId, subject, team, priority, escalated ? ", on-call paged" : "", reasoning);
    }
}
