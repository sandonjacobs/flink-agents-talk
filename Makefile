JAR       := target/ticket-router.jar
# Required by Flink Agents' async execution on JDK 21+, plus the usual Flink opens.
JAVA_OPTS := --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED \
             --add-opens=java.base/java.lang=ALL-UNNAMED \
             --add-opens=java.base/java.util=ALL-UNNAMED \
             --add-opens=java.base/java.time=ALL-UNNAMED
JAVA      := java $(JAVA_OPTS) -cp $(JAR)

.PHONY: up down build job produce interactive watch clean

MODEL     ?= qwen2.5:7b

up:            ## Start Kafka, Schema Registry, Kafka UI; pull + warm the model on native Ollama
	docker compose up -d
	ollama pull $(MODEL)
	ollama run $(MODEL) "Say ready." > /dev/null && echo "$(MODEL) warmed up"

down:          ## Stop the Docker services
	docker compose down

build:         ## Build the fat jar
	mvn -q -DskipTests package

job: $(JAR)    ## Run the Flink job (web UI on http://localhost:8088)
	$(JAVA) io.sandonjacobs.ato.TicketRoutingJob

produce: $(JAR) ## Send the canned sample tickets
	$(JAVA) io.sandonjacobs.ato.tools.TicketProducer

interactive: $(JAR) ## Type tickets live: "subject | description"
	$(JAVA) io.sandonjacobs.ato.tools.TicketProducer --interactive

watch: $(JAR)  ## Tail routing decisions
	$(JAVA) io.sandonjacobs.ato.tools.RoutingConsumer

$(JAR):
	mvn -q -DskipTests package

clean:
	mvn -q clean
