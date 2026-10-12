package io.sandonjacobs.ato.agent;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.sandonjacobs.ato.AppConfig;
import io.sandonjacobs.ato.model.RoutingDecision;
import io.sandonjacobs.ato.model.RoutingVerdict;
import io.sandonjacobs.ato.model.TeamDirectory;
import io.sandonjacobs.ato.model.Ticket;
import org.apache.flink.agents.api.Event;
import org.apache.flink.agents.api.InputEvent;
import org.apache.flink.agents.api.OutputEvent;
import org.apache.flink.agents.api.agents.Agent;
import org.apache.flink.agents.api.annotation.Action;
import org.apache.flink.agents.api.annotation.ChatModelSetup;
import org.apache.flink.agents.api.annotation.Tool;
import org.apache.flink.agents.api.annotation.ToolParam;
import org.apache.flink.agents.api.chat.messages.ChatMessage;
import org.apache.flink.agents.api.chat.messages.MessageRole;
import org.apache.flink.agents.api.context.MemoryObject;
import org.apache.flink.agents.api.context.RunnerContext;
import org.apache.flink.agents.api.event.ChatRequestEvent;
import org.apache.flink.agents.api.event.ChatResponseEvent;
import org.apache.flink.agents.api.prompt.Prompt;
import org.apache.flink.agents.api.resource.ResourceDescriptor;
import org.apache.flink.agents.api.resource.ResourceName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Routes a support ticket to the right team, and pages that team's on-call for P1s.
 *
 * <pre>
 *   InputEvent(Ticket)
 *        │ routeTicket
 *        ▼
 *   ChatRequestEvent ──► "ticketRouter" LLM ──► ChatResponseEvent
 *                                                     │ onChatResponse
 *                          not P1 ◄───────────────────┤
 *                            │                        │ P1
 *                            │                        ▼
 *                            │      ChatRequestEvent ──► "pager" LLM ──► pageOnCall tool
 *                            │                                              │
 *                            │                        ChatResponseEvent ◄───┘
 *                            ▼                              │ onChatResponse
 *   OutputEvent(RoutingDecision) ◄──────────────────────────┘
 * </pre>
 *
 * <p>Two focused LLM steps instead of one do-everything prompt: small local models are reliable at
 * "classify this" and at "call this tool", but flaky at "classify, then maybe call a tool, then
 * answer in JSON".
 */
public class TicketRoutingAgent extends Agent {

    public static final String OLLAMA_CONNECTION = "ollamaConnection";

    private static final Logger LOG = LoggerFactory.getLogger(TicketRoutingAgent.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Set<String> PRIORITIES = Set.of("P1", "P2", "P3", "P4");

    private static final String STAGE = "stage";
    private static final String ROUTING = "routing";
    private static final String PAGING = "paging";

    // ---------------------------------------------------------------- step 1: route the ticket

    private static final String ROUTER_PROMPT =
            "You are the routing agent for a SaaS company's support desk.\n"
                    + "Pick exactly ONE team from this directory to own the ticket:\n\n"
                    + TeamDirectory.asPromptText()
                    + "\n\nAssign a priority:\n"
                    + "- P1: something is broken for many users right now (outage, app crashing for "
                    + "everyone, login broken for a whole company) or a possible security breach\n"
                    + "- P2: one customer is blocked, or money is wrong\n"
                    + "- P3: degraded, a workaround exists, or a non-urgent bug\n"
                    + "- P4: how-to question, feedback, or feature request\n\n"
                    + "Answer with ONLY a JSON object, no markdown, in this shape:\n"
                    + "{\"team\": \"<team id>\", \"priority\": \"P1|P2|P3|P4\", "
                    + "\"reasoning\": \"<one short sentence>\"}";

    @org.apache.flink.agents.api.annotation.Prompt
    public static Prompt routerPrompt() {
        return Prompt.fromMessages(
                List.of(
                        new ChatMessage(MessageRole.SYSTEM, ROUTER_PROMPT),
                        new ChatMessage(MessageRole.USER, "{ticket}")));
    }

    @ChatModelSetup
    public static ResourceDescriptor ticketRouter() {
        return ollama("routerPrompt", List.of());
    }

    // ---------------------------------------------------------------- step 2: escalate P1s

    private static final String PAGER_PROMPT =
            "You are the escalation assistant for a support desk. A ticket has been classified as "
                    + "P1 (critical). Page the owning team's on-call engineer by calling the pageOnCall "
                    + "tool, with a one-line summary an engineer woken at 3am would understand. "
                    + "Call the tool exactly once, then reply with the word: done";

    @org.apache.flink.agents.api.annotation.Prompt
    public static Prompt pagerPrompt() {
        return Prompt.fromMessages(
                List.of(
                        new ChatMessage(MessageRole.SYSTEM, PAGER_PROMPT),
                        new ChatMessage(MessageRole.USER, "Team: {team}\n{ticket}")));
    }

    @ChatModelSetup
    public static ResourceDescriptor pager() {
        return ollama("pagerPrompt", List.of("pageOnCall"));
    }

    /** The tool the pager LLM calls. In real life this would hit PagerDuty / Opsgenie. */
    @Tool(description = "Page the on-call engineer of a team about a critical ticket.")
    public static String pageOnCall(
            @ToolParam(name = "team", description = "The team id to page") String team,
            @ToolParam(name = "summary", description = "One-line summary for the on-call engineer")
                    String summary,
            @ToolParam(name = "ticket_id", description = "The ticket id", required = false)
                    String ticketId) {
        System.out.printf("%n📟  PAGING %s on-call: %s%n%n", team, summary);
        // Be explicit, or the model may "make sure" by paging again.
        return "SUCCESS: " + team + " on-call has been paged. Do not call pageOnCall again. Reply with: done";
    }

    // ---------------------------------------------------------------- actions

    /** A ticket arrives from Kafka: remember it and ask the router LLM where it should go. */
    @Action(listenEventTypes = {InputEvent.EVENT_TYPE})
    public static void routeTicket(Event event, RunnerContext ctx) throws Exception {
        Ticket ticket = MAPPER.convertValue(InputEvent.fromEvent(event).getInput(), Ticket.class);

        String ticketText =
                String.format(
                        "Ticket %s (via %s, customer %s)%nSubject: %s%nDescription: %s",
                        ticket.getTicketId(),
                        ticket.getChannel(),
                        ticket.getCustomerId(),
                        ticket.getSubject(),
                        ticket.getDescription());

        // Sensory memory lives for this one ticket's run of the agent.
        ctx.getSensoryMemory().set("ticket_id", ticket.getTicketId());
        ctx.getSensoryMemory().set("subject", ticket.getSubject());
        ctx.getSensoryMemory().set("ticket_text", ticketText);
        ctx.getSensoryMemory().set(STAGE, ROUTING);

        ctx.sendEvent(chat("ticketRouter", Map.of("ticket", ticketText)));
    }

    /** An LLM answered. Depending on which step we're in, escalate or emit the decision. */
    @Action(listenEventTypes = {ChatResponseEvent.EVENT_TYPE})
    public static void onChatResponse(Event event, RunnerContext ctx) throws Exception {
        if (PAGING.equals(memory(ctx, STAGE))) {
            // The pager LLM has called pageOnCall; we're done.
            emit(ctx, true);
            return;
        }

        String content = ChatResponseEvent.fromEvent(event).getResponse().getContent();
        RoutingVerdict verdict = parseVerdict(content);

        // Never trust the model blindly: unparseable answers and unknown teams go to human triage.
        String team = TeamDirectory.isKnown(verdict.team) ? verdict.team : TeamDirectory.FALLBACK_TEAM;
        String priority =
                verdict.priority != null && PRIORITIES.contains(verdict.priority.toUpperCase())
                        ? verdict.priority.toUpperCase()
                        : "P3";
        ctx.getSensoryMemory().set("team", team);
        ctx.getSensoryMemory().set("priority", priority);
        ctx.getSensoryMemory().set("reasoning", verdict.reasoning == null ? "" : verdict.reasoning);

        if ("P1".equals(priority) && !TeamDirectory.FALLBACK_TEAM.equals(team)) {
            ctx.getSensoryMemory().set(STAGE, PAGING);
            ctx.sendEvent(chat("pager", Map.of("team", team, "ticket", memory(ctx, "ticket_text"))));
        } else {
            emit(ctx, false);
        }
    }

    // ---------------------------------------------------------------- helpers

    private static ResourceDescriptor ollama(String prompt, List<String> tools) {
        return ResourceDescriptor.Builder.newBuilder(ResourceName.ChatModel.OLLAMA_SETUP)
                .addInitialArgument("connection", OLLAMA_CONNECTION)
                .addInitialArgument("model", AppConfig.OLLAMA_MODEL)
                .addInitialArgument("prompt", prompt)
                .addInitialArgument("tools", tools)
                // The Ollama integration defaults to think=true; neither step needs chain-of-thought.
                .addInitialArgument("think", false)
                .build();
    }

    private static ChatRequestEvent chat(String model, Map<String, Object> promptArgs) {
        return new ChatRequestEvent(
                model, List.of(new ChatMessage(MessageRole.USER, "")), promptArgs, null);
    }

    private static void emit(RunnerContext ctx, boolean escalated) throws Exception {
        ctx.sendEvent(
                new OutputEvent(
                        new RoutingDecision(
                                memory(ctx, "ticket_id"),
                                memory(ctx, "subject"),
                                memory(ctx, "team"),
                                memory(ctx, "priority"),
                                memory(ctx, "reasoning"),
                                escalated,
                                AppConfig.OLLAMA_MODEL)));
    }

    /**
     * Small models wrap their JSON in prose, markdown or stray characters, so pull out the first
     * {...} block. If nothing parses, return an empty verdict (which routes to triage).
     */
    static RoutingVerdict parseVerdict(String content) {
        if (content != null) {
            int start = content.indexOf('{');
            int end = content.lastIndexOf('}');
            if (start >= 0 && end > start) {
                try {
                    return MAPPER.readValue(content.substring(start, end + 1), RoutingVerdict.class);
                } catch (Exception ignored) {
                    // fall through
                }
            }
        }
        LOG.warn("Could not parse LLM response, routing to triage: {}", content);
        RoutingVerdict unparsed = new RoutingVerdict();
        unparsed.reasoning = "Agent could not decide; needs a human.";
        return unparsed;
    }

    private static String memory(RunnerContext ctx, String key) throws Exception {
        MemoryObject value = ctx.getSensoryMemory().get(key);
        return value == null ? "" : String.valueOf(value.getValue());
    }
}
