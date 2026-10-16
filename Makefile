JAR       := target/ticket-router.jar
# Required by Flink Agents' async execution on JDK 21+, plus the usual Flink opens.
JAVA_OPTS := --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED \
             --add-opens=java.base/java.lang=ALL-UNNAMED \
             --add-opens=java.base/java.util=ALL-UNNAMED \
             --add-opens=java.base/java.time=ALL-UNNAMED
JAVA      := java $(JAVA_OPTS) -cp $(JAR)

.DEFAULT_GOAL := help
.PHONY: help up down build job produce interactive watch clean

MODEL     ?= qwen2.5:7b
OLLAMA_PID := .ollama.pid
OLLAMA_LOG := .ollama.log

##@ ℹ️  Help

help:          ## 🙋 Show this help
	@awk 'BEGIN {FS = ":.*?## "; printf "\n\033[1m🎫 Flink Agents ticket router\033[0m\n\nUsage: make \033[36m<target>\033[0m\n"} \
		/^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2} \
		/^##@/ {printf "\n\033[1m%s\033[0m\n", substr($$0, 5)}' $(MAKEFILE_LIST)
	@echo

##@ 🏗️  Infrastructure

up:            ## 🚀 Start Kafka, Schema Registry, Kafka UI; start Ollama, pull + warm the model
	docker compose up -d
	@if curl -sf http://localhost:11434/api/version > /dev/null; then \
		echo "ollama already running"; \
	else \
		echo "starting ollama serve (log: $(OLLAMA_LOG))"; \
		nohup ollama serve > $(OLLAMA_LOG) 2>&1 & echo $$! > $(OLLAMA_PID); \
		for i in $$(seq 1 30); do \
			curl -sf http://localhost:11434/api/version > /dev/null && break; \
			sleep 1; \
		done; \
		curl -sf http://localhost:11434/api/version > /dev/null || { echo "ollama failed to start; see $(OLLAMA_LOG)"; exit 1; }; \
	fi
	ollama pull $(MODEL)
	ollama run $(MODEL) "Say ready." > /dev/null && echo "$(MODEL) warmed up"

down:          ## 🛑 Stop the Docker services (and Ollama if `make up` started it)
	docker compose down
	@if [ -f $(OLLAMA_PID) ]; then \
		kill $$(cat $(OLLAMA_PID)) 2>/dev/null || true; \
		rm -f $(OLLAMA_PID); \
		echo "stopped ollama"; \
	fi

##@ 🔨 Build

build:         ## 📦 Build the fat jar
	mvn -q -DskipTests package

clean:         ## 🧹 Remove build artifacts
	mvn -q clean

$(JAR):
	mvn -q -DskipTests package

##@ ⚡ Run

job: $(JAR)    ## 🤖 Run the Flink job (web UI on http://localhost:8088)
	$(JAVA) io.sandonjacobs.ato.TicketRoutingJob

##@ 🎤 Demo

produce: $(JAR) ## 🎟️  Send the canned sample tickets
	$(JAVA) io.sandonjacobs.ato.tools.TicketProducer

interactive: $(JAR) ## ⌨️  Type tickets live: "subject | description"
	$(JAVA) io.sandonjacobs.ato.tools.TicketProducer --interactive

watch: $(JAR)  ## 👀 Tail routing decisions
	$(JAVA) io.sandonjacobs.ato.tools.RoutingConsumer
