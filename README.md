# Flink Agents Ticket Router

Demo for **"Orchestrating Autonomous AI Agents with Apache Kafka and Apache Flink"** — All Things Open 2026.

Support tickets land on Kafka. A [Flink Agents](https://github.com/apache/flink-agents) agent running *inside* a Flink job
asks a local LLM (Ollama) which team should own each ticket and how urgent it is. For P1s, a second LLM step pages
that team's on-call through a tool call. The routing decision goes back to Kafka. Both topics use Avro with Schema Registry.

```
TicketProducer ─► support.tickets (Avro) ─► Flink job ───────────────────────────────────► support.tickets.routed (Avro) ─► RoutingConsumer
                                             │ TicketRoutingAgent                        ▲
                                             │  1. InputEvent ─► "ticketRouter" LLM ─────┤  not P1
                                             │                     (team + priority)     │
                                             │  2. P1 ─► "pager" LLM ─► pageOnCall tool ─┘  escalated = true
                                             └── Ollama on the host (qwen2.5:7b, Metal GPU)
```

| Piece | Where |
|---|---|
| The agent (prompts, model setups, tool, actions) | [`TicketRoutingAgent.java`](src/main/java/io/sandonjacobs/ato/agent/TicketRoutingAgent.java) |
| The Flink job (Kafka source → agent → Kafka sink) | [`TicketRoutingJob.java`](src/main/java/io/sandonjacobs/ato/TicketRoutingJob.java) |
| Avro contracts | [`src/main/avro`](src/main/avro) |
| Team directory the LLM chooses from | [`teams.json`](src/main/resources/teams.json) |
| Canned demo tickets | [`sample-tickets.json`](src/main/resources/sample-tickets.json) |

**Versions:** Java 21, Ollama 0.40 (native), Flink 2.2.1, Flink Agents 0.3.1, flink-connector-kafka 5.0.0-2.2, Kafka 4.3, Confluent Schema Registry 8.1.

## Run it

```bash
brew install ollama && brew services start ollama   # native Ollama, uses the Mac's GPU
mise install          # Java 21 + Maven 3.9 (see mise.toml)
make up               # Kafka, Schema Registry, Kafka UI (Docker); pulls + warms qwen2.5:7b
make build
```

Then, in three terminals:

```bash
make job              # Flink job (MiniCluster) — Flink UI at http://localhost:8088
make watch            # tail routing decisions from Kafka
make produce          # send the 8 sample tickets   (or: make interactive)
```

| UI | URL |
|---|---|
| Flink web UI | http://localhost:8088 |
| Kafka UI (topics, Avro messages, schemas) | http://localhost:9080 |
| Schema Registry | http://localhost:8081 |

`make interactive` lets you type `subject | description` — good for taking a ticket from the audience.

### Configuration

All via environment variables (see [`AppConfig.java`](src/main/java/io/sandonjacobs/ato/AppConfig.java)):
`KAFKA_BOOTSTRAP_SERVERS`, `SCHEMA_REGISTRY_URL`, `OLLAMA_ENDPOINT` (default `http://localhost:11434`), `OLLAMA_MODEL` (default `qwen2.5:7b`), `FLINK_UI_PORT`.

To see every agent event (input, chat request/response, tool call, output) as JSON, set
`logger.agents.level = INFO` in [`log4j2.properties`](src/main/resources/log4j2.properties).

## Demo runbook (7–10 min)

1. **(1 min)** Show `docker compose ps`, the Kafka UI topics, and the two Avro schemas in Schema Registry.
2. **(2 min)** Walk through `TicketRoutingAgent`: the two `@Prompt`/`@ChatModelSetup` pairs (router and pager),
   the `@Tool`, and the two `@Action`s — event-driven steps, not one big request/response.
3. **(1 min)** `make job`, open the Flink UI — the agent is just another operator in the DAG.
4. **(2 min)** `make produce` with `make watch` alongside — 8 tickets routed in ~15 s; the outage and the security
   breach page on-call (📟).
5. **(2 min)** `make interactive` — take a ticket from the audience.
6. **(optional)** Kill the job mid-stream and restart it: Flink's checkpointed state and Kafka offsets mean nothing is lost.

## Notes and gotchas (learned the hard way)

- **Run Ollama natively on macOS.** Docker on a Mac can't use the Metal GPU: `qwen3:4b` took ~90 s per ticket on
  CPU, versus ~1–2 s for `qwen2.5:7b` natively. (If you must, `docker compose --profile docker-ollama up -d` still
  runs it in Docker — stop native Ollama first, they share port 11434.)
- **Avoid reasoning models for this.** `qwen3` leaked its chain-of-thought into the answer even with `think=false`,
  which broke JSON parsing.
- **One job per LLM call.** A single prompt that had to classify, *maybe* call a tool, then answer in JSON was flaky
  (the tool fired for 1 in 4 P1s). Splitting it into a router step and a pager step made both reliable (24/24 routed,
  6/6 P1s paged once). The agent's event-driven actions make that split cheap.
- **Tell the model the tool worked.** A bare "Paged." tool result made it page again "to be sure" — the tool now
  returns an explicit "SUCCESS ... do not call again".
- The Flink Agents Ollama integration **defaults to `think=true`** — the agent sets `think=false` explicitly.
- The decision to page is made in code (`priority == P1`); the LLM only writes the page and calls the tool.
- The agent parses the LLM's JSON leniently and routes anything unparseable or unknown to `triage` instead of failing
  the job. (An exception in an action fails the Flink task, which restarts and replays the same ticket — forever.)
- JDK 21 needs `--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED` for Flink Agents' async execution; the
  `Makefile` passes it.
- Confluent's `logredactor` jar ships a Log4j plugin cache that silently breaks all logging in a shaded jar; the
  shade config filters it out.
