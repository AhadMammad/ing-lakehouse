# ═══════════════════════════════════════════════════════════════════
#  ing-lakehouse — Makefile
#  Top-level orchestration for the entire lakehouse platform.
#  Run `make` or `make help` to see all available targets.
# ═══════════════════════════════════════════════════════════════════

# ── ANSI color codes ───────────────────────────────────────────────
GREEN  := \033[0;32m
YELLOW := \033[1;33m
CYAN   := \033[0;36m
RED    := \033[0;31m
BLUE   := \033[0;34m
BOLD   := \033[1m
DIM    := \033[2m
RESET  := \033[0m

# ── Instance isolation ─────────────────────────────────────────────
# Each Unix user (or explicit INSTANCE=name) gets its own Docker
# project name, network, volumes, and port set.  Run once per user:
#   make init-instance            (uses $(whoami))
#   make init-instance INSTANCE=alice
INSTANCE     ?= $(shell whoami)
INSTANCE_ENV := .env.$(INSTANCE)
PROJECT_NAME := ing-lakehouse-$(INSTANCE)
NETWORK_NAME  = $(PROJECT_NAME)_lakehouse-net

# Load base .env first, then instance env (overrides ports/project name)
-include .env
-include $(INSTANCE_ENV)

# Env file passed to docker compose for variable substitution
_ENV_FILE    := $(or $(wildcard $(INSTANCE_ENV)),.env)

# ── Compose file selectors ─────────────────────────────────────────
COMPOSE_LOCAL := docker compose --project-name $(PROJECT_NAME) --env-file $(_ENV_FILE) -f docker-compose.local.yml
COMPOSE_SSL   := docker compose --project-name $(PROJECT_NAME) --env-file $(_ENV_FILE) -f docker-compose.local.ssl.yml
COMPOSE_DIST  := docker compose --project-name $(PROJECT_NAME) --env-file $(_ENV_FILE) -f docker-compose.yml

# ── Service / node lists ───────────────────────────────────────────
NODE               ?= rustfs
RUSTFS_DIST_NODES  := rustfs1 rustfs2 rustfs3 rustfs4
KAFKA_DIST_BROKERS := kafka-1 kafka-2 kafka-3
SPARK_WORKERS      := spark-worker-1 spark-worker-2

# Derive display variables from whichever env file is active
S3_PORT      := $(or $(RUSTFS_S3_PORT),9000)
CONSOLE_PORT := $(or $(RUSTFS_CONSOLE_PORT),9001)
SPARK_UI     := $(or $(SPARK_MASTER_UI_PORT),8080)
KAFKA_PORT   := $(or $(KAFKA_BROKER_PORT),9092)
RUSTFS_HOST  := $(or $(RUSTFS_DOMAIN),rustfs.lakehouse.local)
NESSIE_PORT_VAR          := $(or $(NESSIE_PORT),19120)
JUPYTER_PORT_VAR         := $(or $(JUPYTER_PORT),8888)
ICEBERG_WAREHOUSE_BUCKET := $(or $(ICEBERG_WAREHOUSE_BUCKET),iceberg-warehouse)

.DEFAULT_GOAL := help
.PHONY: help \
        up up-ssl up-dist down down-ssl down-dist restart restart-ssl restart-dist \
        pull pull-dist clean clean-ssl clean-dist \
        status status-dist ps \
        logs logs-dist logs-rustfs logs-rustfs-dist logs-spark logs-spark-dist \
        logs-kafka logs-kafka-dist logs-nessie logs-jupyter logs-nginx logs-node \
        health health-dist console console-ssl console-dist network \
        nessie-init-bucket \
        jupyter-rebuild reset-events-table reset-nessie \
        setup-certs init-instance

# ── Help ───────────────────────────────────────────────────────────
help:
	@printf "\n$(BOLD)$(CYAN)╔══════════════════════════════════════════════════════╗$(RESET)\n"
	@printf "$(BOLD)$(CYAN)║          ing-lakehouse  —  make targets              ║$(RESET)\n"
	@printf "$(BOLD)$(CYAN)╚══════════════════════════════════════════════════════╝$(RESET)\n\n"
	@printf "$(BOLD)  Multi-instance (run once per user on shared VMs)$(RESET)\n"
	@printf "  $(CYAN)init-instance$(RESET)    $(DIM)Create .env.<INSTANCE> with auto-allocated ports (INSTANCE=alice)$(RESET)\n"
	@printf "\n$(BOLD)  Local plain HTTP (single-node) — default for development$(RESET)\n"
	@printf "  $(GREEN)up$(RESET)               $(DIM)Start all services in single-node mode (plain HTTP, no cert)$(RESET)\n"
	@printf "  $(RED)down$(RESET)             $(DIM)Stop local services (keeps volumes)$(RESET)\n"
	@printf "  $(YELLOW)restart$(RESET)          $(DIM)Restart local services$(RESET)\n"
	@printf "  $(YELLOW)pull$(RESET)             $(DIM)Pull latest images for local mode$(RESET)\n"
	@printf "  $(RED)clean$(RESET)            $(DIM)Stop local services AND delete volumes  ⚠ destructive$(RESET)\n"
	@printf "\n$(BOLD)  Local SSL (single-node + shared NGINX proxy)$(RESET)\n"
	@printf "  $(GREEN)up-ssl$(RESET)           $(DIM)Start with NGINX SSL proxy (run setup-certs first)$(RESET)\n"
	@printf "  $(RED)down-ssl$(RESET)         $(DIM)Stop SSL stack (keeps volumes)$(RESET)\n"
	@printf "  $(YELLOW)restart-ssl$(RESET)      $(DIM)Restart SSL stack$(RESET)\n"
	@printf "  $(RED)clean-ssl$(RESET)        $(DIM)Stop SSL stack AND delete volumes  ⚠ destructive$(RESET)\n"
	@printf "\n$(BOLD)  Distributed — full cluster for demos$(RESET)\n"
	@printf "  $(GREEN)up-dist$(RESET)          $(DIM)Start all services in distributed mode$(RESET)\n"
	@printf "  $(RED)down-dist$(RESET)        $(DIM)Stop distributed services (keeps volumes)$(RESET)\n"
	@printf "  $(YELLOW)restart-dist$(RESET)     $(DIM)Restart distributed services$(RESET)\n"
	@printf "  $(YELLOW)pull-dist$(RESET)        $(DIM)Pull latest images for distributed mode$(RESET)\n"
	@printf "  $(RED)clean-dist$(RESET)       $(DIM)Stop distributed services AND delete volumes  ⚠ destructive$(RESET)\n"
	@printf "\n$(BOLD)  Observability$(RESET)\n"
	@printf "  $(CYAN)status$(RESET)           $(DIM)Show local container status$(RESET)\n"
	@printf "  $(CYAN)status-dist$(RESET)      $(DIM)Show distributed container status$(RESET)\n"
	@printf "  $(BLUE)logs$(RESET)             $(DIM)Stream all local logs$(RESET)\n"
	@printf "  $(BLUE)logs-dist$(RESET)        $(DIM)Stream all distributed logs$(RESET)\n"
	@printf "  $(BLUE)logs-rustfs$(RESET)      $(DIM)RustFS logs (local)$(RESET)\n"
	@printf "  $(BLUE)logs-rustfs-dist$(RESET) $(DIM)RustFS node logs (distributed)$(RESET)\n"
	@printf "  $(BLUE)logs-spark$(RESET)       $(DIM)Spark master logs (local)$(RESET)\n"
	@printf "  $(BLUE)logs-spark-dist$(RESET)  $(DIM)Spark master + worker logs (distributed)$(RESET)\n"
	@printf "  $(BLUE)logs-kafka$(RESET)       $(DIM)Kafka logs (local)$(RESET)\n"
	@printf "  $(BLUE)logs-kafka-dist$(RESET)  $(DIM)Kafka broker logs (distributed)$(RESET)\n"
	@printf "  $(BLUE)logs-nessie$(RESET)      $(DIM)Nessie catalog logs (local)$(RESET)\n"
	@printf "  $(BLUE)logs-jupyter$(RESET)     $(DIM)Jupyter notebook server logs (local)$(RESET)\n"
	@printf "  $(BLUE)logs-nginx$(RESET)       $(DIM)NGINX proxy logs (SSL mode)$(RESET)\n"
	@printf "  $(BLUE)logs-node$(RESET)        $(DIM)Logs for one container  (NODE=kafka-2)$(RESET)\n"
	@printf "  $(GREEN)health$(RESET)           $(DIM)Health check all local services$(RESET)\n"
	@printf "  $(GREEN)health-dist$(RESET)      $(DIM)Health check all distributed services$(RESET)\n"
	@printf "  $(CYAN)network$(RESET)          $(DIM)List containers on lakehouse-net$(RESET)\n"
	@printf "\n$(BOLD)  Access$(RESET)\n"
	@printf "  $(CYAN)console$(RESET)          $(DIM)Print local endpoints and credentials (plain HTTP)$(RESET)\n"
	@printf "  $(CYAN)console-ssl$(RESET)      $(DIM)Print SSL endpoints and credentials$(RESET)\n"
	@printf "  $(CYAN)console-dist$(RESET)     $(DIM)Print distributed endpoints and credentials$(RESET)\n"
	@printf "  $(CYAN)setup-certs$(RESET)      $(DIM)Generate SSL certs for NGINX proxy (requires mkcert)$(RESET)\n"
	@printf "\n$(BOLD)  Iceberg / Curriculum$(RESET)\n"
	@printf "  $(CYAN)nessie-init-bucket$(RESET)   $(DIM)Create Iceberg warehouse bucket in RustFS (run once after make up)$(RESET)\n"
	@printf "  $(CYAN)jupyter-rebuild$(RESET)      $(DIM)Rebuild Jupyter image after Dockerfile changes$(RESET)\n"
	@printf "  $(CYAN)reset-events-table$(RESET)   $(DIM)Drop demo.events + prune S3 prefix — replay 01–10 cleanly$(RESET)\n"
	@printf "  $(RED)reset-nessie$(RESET)         $(DIM)Wipe Nessie state (volume) and re-init bucket  ⚠ destructive$(RESET)\n"
	@printf "\n$(DIM)  Variables: INSTANCE=<name> (default: current user)  NODE=<container-name>$(RESET)\n\n"

# ── Multi-instance init ────────────────────────────────────────────
init-instance:
	@INSTANCE="$(INSTANCE)"; \
	ENV_FILE=".env.$$INSTANCE"; \
	if [ -f "$$ENV_FILE" ]; then \
		printf "$(YELLOW)⚠  $$ENV_FILE already exists. Delete it first to reinit.$(RESET)\n"; \
		exit 1; \
	fi; \
	printf "$(BOLD)$(CYAN)▶  Allocating ports for instance '$$INSTANCE'...$(RESET)\n"; \
	PORTS=$$(python3 scripts/find-free-ports.py); \
	S3=$$(echo "$$PORTS"   | awk '{print $$1}'); \
	CON=$$(echo "$$PORTS"  | awk '{print $$2}'); \
	SPUI=$$(echo "$$PORTS" | awk '{print $$3}'); \
	SPMAS=$$(echo "$$PORTS"| awk '{print $$4}'); \
	KAF=$$(echo "$$PORTS"  | awk '{print $$5}'); \
	NES=$$(echo "$$PORTS"  | awk '{print $$6}'); \
	JUP=$$(echo "$$PORTS"  | awk '{print $$7}'); \
	{ printf "COMPOSE_PROJECT_NAME=ing-lakehouse-$$INSTANCE\n\n"; \
	  grep -v "^COMPOSE_PROJECT_NAME" .env; } > "$$ENV_FILE"; \
	sed -i "s|^RUSTFS_S3_PORT=.*|RUSTFS_S3_PORT=$$S3|" "$$ENV_FILE"; \
	sed -i "s|^RUSTFS_CONSOLE_PORT=.*|RUSTFS_CONSOLE_PORT=$$CON|" "$$ENV_FILE"; \
	sed -i "s|^SPARK_MASTER_UI_PORT=.*|SPARK_MASTER_UI_PORT=$$SPUI|" "$$ENV_FILE"; \
	sed -i "s|^SPARK_MASTER_PORT=.*|SPARK_MASTER_PORT=$$SPMAS|" "$$ENV_FILE"; \
	sed -i "s|^KAFKA_BROKER_PORT=.*|KAFKA_BROKER_PORT=$$KAF|" "$$ENV_FILE"; \
	sed -i "s|^NESSIE_PORT=.*|NESSIE_PORT=$$NES|" "$$ENV_FILE"; \
	sed -i "s|^JUPYTER_PORT=.*|JUPYTER_PORT=$$JUP|" "$$ENV_FILE"; \
	printf "$(GREEN)✔  Created $$ENV_FILE$(RESET)\n"; \
	printf "$(DIM)   Project    → ing-lakehouse-$$INSTANCE$(RESET)\n"; \
	printf "$(DIM)   RustFS S3  → :$$S3    Console → :$$CON$(RESET)\n"; \
	printf "$(DIM)   Spark UI   → :$$SPUI  Master  → :$$SPMAS$(RESET)\n"; \
	printf "$(DIM)   Kafka      → :$$KAF$(RESET)\n"; \
	printf "$(DIM)   Nessie     → :$$NES$(RESET)\n"; \
	printf "$(DIM)   Jupyter    → :$$JUP$(RESET)\n"; \
	printf "\n$(DIM)   Next: make up$(RESET)\n"

# ── Lifecycle: local ───────────────────────────────────────────────
up:
	@printf "$(BOLD)$(GREEN)▶  Starting ing-lakehouse (local, plain HTTP) [instance: $(INSTANCE)]...$(RESET)\n"
	@$(COMPOSE_LOCAL) up -d --build --remove-orphans
	@printf "$(GREEN)✔  Local services started.$(RESET)\n"
	@printf "$(DIM)   RustFS S3    → http://localhost:$(S3_PORT)$(RESET)\n"
	@printf "$(DIM)   RustFS UI    → http://localhost:$(CONSOLE_PORT)$(RESET)\n"
	@printf "$(DIM)   Spark UI     → http://localhost:$(SPARK_UI)$(RESET)\n"
	@printf "$(DIM)   Kafka        → localhost:$(KAFKA_PORT)$(RESET)\n"
	@printf "$(DIM)   Nessie       → http://localhost:$(NESSIE_PORT_VAR)$(RESET)\n"
	@printf "$(DIM)   Jupyter      → http://localhost:$(JUPYTER_PORT_VAR)?token=$(JUPYTER_TOKEN)$(RESET)\n"

down:
	@printf "$(BOLD)$(RED)▶  Stopping ing-lakehouse (local) [instance: $(INSTANCE)]...$(RESET)\n"
	@$(COMPOSE_LOCAL) down
	@printf "$(RED)✔  Local services stopped.$(RESET)\n"

restart:
	@printf "$(BOLD)$(YELLOW)▶  Restarting ing-lakehouse (local) [instance: $(INSTANCE)]...$(RESET)\n"
	@$(COMPOSE_LOCAL) restart
	@printf "$(YELLOW)✔  Local services restarted.$(RESET)\n"

pull:
	@printf "$(BOLD)$(YELLOW)▶  Pulling latest images (local)...$(RESET)\n"
	@$(COMPOSE_LOCAL) pull
	@printf "$(YELLOW)✔  Images updated.$(RESET)\n"

clean:
	@printf "$(BOLD)$(RED)⚠  WARNING: This will destroy all local data volumes for instance '$(INSTANCE)'!$(RESET)\n"
	@printf "$(RED)   Press Ctrl-C within 5s to abort...$(RESET)\n"
	@sleep 5
	@$(COMPOSE_LOCAL) down -v --remove-orphans
	@printf "$(RED)✔  Local volumes deleted.$(RESET)\n"

# ── Lifecycle: SSL (local + NGINX proxy) ──────────────────────────
up-ssl:
	@printf "$(BOLD)$(GREEN)▶  Starting ing-lakehouse (SSL) [instance: $(INSTANCE)]...$(RESET)\n"
	@$(COMPOSE_SSL) up -d --build --remove-orphans
	@printf "$(GREEN)✔  SSL services started.$(RESET)\n"
	@printf "$(DIM)   RustFS S3    → https://$(RUSTFS_HOST):$(S3_PORT)$(RESET)\n"
	@printf "$(DIM)   RustFS UI    → https://$(RUSTFS_HOST):$(CONSOLE_PORT)$(RESET)\n"
	@printf "$(DIM)   Spark UI     → https://localhost:$(SPARK_UI)$(RESET)\n"
	@printf "$(DIM)   Kafka        → localhost:$(KAFKA_PORT)$(RESET)\n"
	@printf "$(DIM)   Nessie       → https://localhost:$(NESSIE_PORT_VAR)$(RESET)\n"
	@printf "$(DIM)   Jupyter      → https://localhost:$(JUPYTER_PORT_VAR)?token=$(JUPYTER_TOKEN)$(RESET)\n"

down-ssl:
	@printf "$(BOLD)$(RED)▶  Stopping ing-lakehouse (SSL) [instance: $(INSTANCE)]...$(RESET)\n"
	@$(COMPOSE_SSL) down
	@printf "$(RED)✔  SSL services stopped.$(RESET)\n"

restart-ssl:
	@printf "$(BOLD)$(YELLOW)▶  Restarting ing-lakehouse (SSL) [instance: $(INSTANCE)]...$(RESET)\n"
	@$(COMPOSE_SSL) restart
	@printf "$(YELLOW)✔  SSL services restarted.$(RESET)\n"

clean-ssl:
	@printf "$(BOLD)$(RED)⚠  WARNING: This will destroy all SSL stack data volumes for instance '$(INSTANCE)'!$(RESET)\n"
	@printf "$(RED)   Press Ctrl-C within 5s to abort...$(RESET)\n"
	@sleep 5
	@$(COMPOSE_SSL) down -v --remove-orphans
	@printf "$(RED)✔  SSL volumes deleted.$(RESET)\n"

# ── Lifecycle: distributed ─────────────────────────────────────────
up-dist:
	@printf "$(BOLD)$(GREEN)▶  Starting ing-lakehouse (distributed) [instance: $(INSTANCE)]...$(RESET)\n"
	@$(COMPOSE_DIST) up -d --build --remove-orphans
	@printf "$(GREEN)✔  Distributed services started.$(RESET)\n"
	@printf "$(DIM)   RustFS S3    → https://$(RUSTFS_HOST):$(S3_PORT)  (nginx LB)$(RESET)\n"
	@printf "$(DIM)   RustFS UI    → https://$(RUSTFS_HOST):$(CONSOLE_PORT)$(RESET)\n"
	@printf "$(DIM)   Spark UI     → http://localhost:$(SPARK_UI)$(RESET)\n"
	@printf "$(DIM)   Kafka        → localhost:$(KAFKA_PORT),localhost:$(KAFKA_BROKER2_PORT),localhost:$(KAFKA_BROKER3_PORT)$(RESET)\n"

down-dist:
	@printf "$(BOLD)$(RED)▶  Stopping ing-lakehouse (distributed) [instance: $(INSTANCE)]...$(RESET)\n"
	@$(COMPOSE_DIST) down
	@printf "$(RED)✔  Distributed services stopped.$(RESET)\n"

restart-dist:
	@printf "$(BOLD)$(YELLOW)▶  Restarting ing-lakehouse (distributed) [instance: $(INSTANCE)]...$(RESET)\n"
	@$(COMPOSE_DIST) restart
	@printf "$(YELLOW)✔  Distributed services restarted.$(RESET)\n"

pull-dist:
	@printf "$(BOLD)$(YELLOW)▶  Pulling latest images (distributed)...$(RESET)\n"
	@$(COMPOSE_DIST) pull
	@printf "$(YELLOW)✔  Images updated.$(RESET)\n"

clean-dist:
	@printf "$(BOLD)$(RED)⚠  WARNING: This will destroy all distributed data volumes for instance '$(INSTANCE)'!$(RESET)\n"
	@printf "$(RED)   Press Ctrl-C within 5s to abort...$(RESET)\n"
	@sleep 5
	@$(COMPOSE_DIST) down -v --remove-orphans
	@printf "$(RED)✔  Distributed volumes deleted.$(RESET)\n"

# ── Observability: local ───────────────────────────────────────────
status ps:
	@printf "$(BOLD)$(CYAN)▶  ing-lakehouse (local) [instance: $(INSTANCE)] — container status$(RESET)\n\n"
	@$(COMPOSE_LOCAL) ps

logs:
	@printf "$(BOLD)$(BLUE)▶  Streaming all local logs (Ctrl-C to exit)...$(RESET)\n"
	@$(COMPOSE_LOCAL) logs -f --tail=100

logs-rustfs:
	@printf "$(BOLD)$(BLUE)▶  Streaming RustFS logs (local, Ctrl-C to exit)...$(RESET)\n"
	@$(COMPOSE_LOCAL) logs -f --tail=100 rustfs

logs-spark:
	@printf "$(BOLD)$(BLUE)▶  Streaming Spark master logs (local, Ctrl-C to exit)...$(RESET)\n"
	@$(COMPOSE_LOCAL) logs -f --tail=100 spark-master

logs-kafka:
	@printf "$(BOLD)$(BLUE)▶  Streaming Kafka logs (local, Ctrl-C to exit)...$(RESET)\n"
	@$(COMPOSE_LOCAL) logs -f --tail=100 kafka

logs-nessie:
	@printf "$(BOLD)$(BLUE)▶  Streaming Nessie logs (local, Ctrl-C to exit)...$(RESET)\n"
	@$(COMPOSE_LOCAL) logs -f --tail=100 nessie

logs-jupyter:
	@printf "$(BOLD)$(BLUE)▶  Streaming Jupyter logs (local, Ctrl-C to exit)...$(RESET)\n"
	@$(COMPOSE_LOCAL) logs -f --tail=100 jupyter

logs-nginx:
	@printf "$(BOLD)$(BLUE)▶  Streaming NGINX proxy logs (SSL mode, Ctrl-C to exit)...$(RESET)\n"
	@$(COMPOSE_SSL) logs -f --tail=100 nginx

# ── Observability: distributed ─────────────────────────────────────
status-dist:
	@printf "$(BOLD)$(CYAN)▶  ing-lakehouse (distributed) [instance: $(INSTANCE)] — container status$(RESET)\n\n"
	@$(COMPOSE_DIST) ps

logs-dist:
	@printf "$(BOLD)$(BLUE)▶  Streaming all distributed logs (Ctrl-C to exit)...$(RESET)\n"
	@$(COMPOSE_DIST) logs -f --tail=100

logs-rustfs-dist:
	@printf "$(BOLD)$(BLUE)▶  Streaming RustFS node logs (distributed, Ctrl-C to exit)...$(RESET)\n"
	@$(COMPOSE_DIST) logs -f --tail=100 rustfs1 rustfs2 rustfs3 rustfs4

logs-spark-dist:
	@printf "$(BOLD)$(BLUE)▶  Streaming Spark logs (distributed, Ctrl-C to exit)...$(RESET)\n"
	@$(COMPOSE_DIST) logs -f --tail=100 spark-master $(SPARK_WORKERS)

logs-kafka-dist:
	@printf "$(BOLD)$(BLUE)▶  Streaming Kafka broker logs (distributed, Ctrl-C to exit)...$(RESET)\n"
	@$(COMPOSE_DIST) logs -f --tail=100 $(KAFKA_DIST_BROKERS)

logs-node:
	@printf "$(BOLD)$(BLUE)▶  Streaming logs for $(NODE) (Ctrl-C to exit)...$(RESET)\n"
	@$(COMPOSE_LOCAL) logs -f --tail=100 $(NODE)

# ── Health checks ──────────────────────────────────────────────────
health:
	@printf "$(BOLD)$(GREEN)▶  Checking local service health [instance: $(INSTANCE)]...$(RESET)\n\n"
	@for svc in rustfs spark-master kafka nessie jupyter; do \
		container="$(PROJECT_NAME)-$$svc-1"; \
		printf "  $(CYAN)$$container$(RESET)  "; \
		status=$$(docker inspect --format='{{.State.Health.Status}}' "$$container" 2>/dev/null || echo "not found"); \
		if [ "$$status" = "healthy" ]; then \
			printf "$(GREEN)● healthy$(RESET)\n"; \
		else \
			printf "$(RED)● $$status$(RESET)\n"; \
		fi; \
	done
	@printf "\n"

health-dist:
	@printf "$(BOLD)$(GREEN)▶  Checking distributed service health [instance: $(INSTANCE)]...$(RESET)\n\n"
	@printf "$(BOLD)  RustFS nodes$(RESET)\n"
	@for node in $(RUSTFS_DIST_NODES); do \
		container="$(PROJECT_NAME)-$$node-1"; \
		printf "  $(CYAN)$$container$(RESET)  "; \
		status=$$(docker inspect --format='{{.State.Health.Status}}' "$$container" 2>/dev/null || echo "not found"); \
		http=$$(docker exec "$$container" curl -sf -o /dev/null -w "%{http_code}" http://localhost:9000/health 2>/dev/null || echo "ERR"); \
		if [ "$$status" = "healthy" ] && [ "$$http" = "200" ]; then \
			printf "$(GREEN)● healthy$(RESET)  HTTP $(GREEN)$$http$(RESET)\n"; \
		else \
			printf "$(RED)● $$status$(RESET)  HTTP $(RED)$$http$(RESET)\n"; \
		fi; \
	done
	@printf "\n$(BOLD)  Spark$(RESET)\n"
	@for svc in spark-master; do \
		container="$(PROJECT_NAME)-$$svc-1"; \
		printf "  $(CYAN)$$container$(RESET)  "; \
		status=$$(docker inspect --format='{{.State.Health.Status}}' "$$container" 2>/dev/null || echo "not found"); \
		if [ "$$status" = "healthy" ]; then \
			printf "$(GREEN)● healthy$(RESET)\n"; \
		else \
			printf "$(RED)● $$status$(RESET)\n"; \
		fi; \
	done
	@printf "\n$(BOLD)  Kafka brokers$(RESET)\n"
	@for broker in $(KAFKA_DIST_BROKERS); do \
		container="$(PROJECT_NAME)-$$broker-1"; \
		printf "  $(CYAN)$$container$(RESET)  "; \
		status=$$(docker inspect --format='{{.State.Health.Status}}' "$$container" 2>/dev/null || echo "not found"); \
		if [ "$$status" = "healthy" ]; then \
			printf "$(GREEN)● healthy$(RESET)\n"; \
		else \
			printf "$(RED)● $$status$(RESET)\n"; \
		fi; \
	done
	@printf "\n"

# ── Network ────────────────────────────────────────────────────────
network:
	@printf "$(BOLD)$(CYAN)▶  Containers on $(NETWORK_NAME)$(RESET)\n\n"
	@docker network inspect $(NETWORK_NAME) \
		--format '{{range .Containers}}  {{.Name}}  ({{.IPv4Address}}){{"\n"}}{{end}}' 2>/dev/null \
		|| printf "$(RED)  Network not found — run 'make up' first$(RESET)\n"
	@printf "\n"

# ── Access / console ───────────────────────────────────────────────
console:
	@printf "\n$(BOLD)$(CYAN)╔══════════════════════════════════════════╗$(RESET)\n"
	@printf "$(BOLD)$(CYAN)║   ing-lakehouse — Local Endpoints (HTTP) ║$(RESET)\n"
	@printf "$(BOLD)$(CYAN)╚══════════════════════════════════════════╝$(RESET)\n\n"
	@printf "  $(BOLD)Instance$(RESET)     $(INSTANCE)\n\n"
	@printf "  $(BOLD)RustFS S3$(RESET)    http://localhost:$(S3_PORT)\n"
	@printf "  $(BOLD)RustFS UI$(RESET)    http://localhost:$(CONSOLE_PORT)\n"
	@printf "  $(BOLD)User$(RESET)         $(RUSTFS_ACCESS_KEY)\n"
	@printf "  $(BOLD)Password$(RESET)     $(RUSTFS_SECRET_KEY)\n\n"
	@printf "  $(BOLD)Spark UI$(RESET)     http://localhost:$(SPARK_UI)\n"
	@printf "  $(BOLD)Kafka$(RESET)        localhost:$(KAFKA_PORT)\n\n"
	@printf "  $(BOLD)Nessie$(RESET)       http://localhost:$(NESSIE_PORT_VAR)\n"
	@printf "  $(BOLD)Jupyter$(RESET)      http://localhost:$(JUPYTER_PORT_VAR)?token=$(JUPYTER_TOKEN)\n\n"
	@printf "  $(DIM)aws s3 --endpoint-url http://localhost:$(S3_PORT) ls$(RESET)\n\n"

console-ssl:
	@printf "\n$(BOLD)$(CYAN)╔══════════════════════════════════════════╗$(RESET)\n"
	@printf "$(BOLD)$(CYAN)║   ing-lakehouse — SSL Endpoints (HTTPS)  ║$(RESET)\n"
	@printf "$(BOLD)$(CYAN)╚══════════════════════════════════════════╝$(RESET)\n\n"
	@printf "  $(BOLD)Instance$(RESET)     $(INSTANCE)\n\n"
	@printf "  $(BOLD)RustFS S3$(RESET)    https://$(RUSTFS_HOST):$(S3_PORT)\n"
	@printf "  $(BOLD)RustFS UI$(RESET)    https://$(RUSTFS_HOST):$(CONSOLE_PORT)\n"
	@printf "  $(BOLD)User$(RESET)         $(RUSTFS_ACCESS_KEY)\n"
	@printf "  $(BOLD)Password$(RESET)     $(RUSTFS_SECRET_KEY)\n\n"
	@printf "  $(BOLD)Spark UI$(RESET)     https://localhost:$(SPARK_UI)\n"
	@printf "  $(BOLD)Kafka$(RESET)        localhost:$(KAFKA_PORT)\n\n"
	@printf "  $(BOLD)Nessie$(RESET)       https://localhost:$(NESSIE_PORT_VAR)\n"
	@printf "  $(BOLD)Jupyter$(RESET)      https://localhost:$(JUPYTER_PORT_VAR)?token=$(JUPYTER_TOKEN)\n\n"
	@printf "  $(DIM)aws s3 --endpoint-url https://$(RUSTFS_HOST):$(S3_PORT) ls$(RESET)\n\n"

console-dist:
	@printf "\n$(BOLD)$(CYAN)╔══════════════════════════════════════════╗$(RESET)\n"
	@printf "$(BOLD)$(CYAN)║   ing-lakehouse — Distributed Endpoints  ║$(RESET)\n"
	@printf "$(BOLD)$(CYAN)╚══════════════════════════════════════════╝$(RESET)\n\n"
	@printf "  $(BOLD)Instance$(RESET)     $(INSTANCE)\n\n"
	@printf "  $(BOLD)RustFS S3$(RESET)    https://$(RUSTFS_HOST):$(S3_PORT)  (nginx LB → 4 nodes)\n"
	@printf "  $(BOLD)RustFS UI$(RESET)    https://$(RUSTFS_HOST):$(CONSOLE_PORT)\n"
	@printf "  $(BOLD)User$(RESET)         $(RUSTFS_ACCESS_KEY)\n"
	@printf "  $(BOLD)Password$(RESET)     $(RUSTFS_SECRET_KEY)\n\n"
	@printf "  $(BOLD)Spark UI$(RESET)     http://localhost:$(SPARK_UI)  (master + 2 workers)\n\n"
	@printf "  $(BOLD)Kafka brokers$(RESET)\n"
	@printf "    kafka-1  localhost:$(KAFKA_PORT)\n"
	@printf "    kafka-2  localhost:$(KAFKA_BROKER2_PORT)\n"
	@printf "    kafka-3  localhost:$(KAFKA_BROKER3_PORT)\n\n"
	@printf "  $(DIM)Bootstrap: localhost:$(KAFKA_PORT),localhost:$(KAFKA_BROKER2_PORT),localhost:$(KAFKA_BROKER3_PORT)$(RESET)\n\n"

# ── Iceberg / Nessie ───────────────────────────────────────────────
nessie-init-bucket:
	@printf "$(BOLD)$(CYAN)▶  Creating Iceberg warehouse bucket '$(ICEBERG_WAREHOUSE_BUCKET)' in RustFS [instance: $(INSTANCE)]...$(RESET)\n"
	@docker run --rm \
		--network $(NETWORK_NAME) \
		-e AWS_ACCESS_KEY_ID=$(RUSTFS_ACCESS_KEY) \
		-e AWS_SECRET_ACCESS_KEY=$(RUSTFS_SECRET_KEY) \
		-e AWS_DEFAULT_REGION=us-east-1 \
		amazon/aws-cli:latest \
		s3api head-bucket \
		--bucket $(ICEBERG_WAREHOUSE_BUCKET) \
		--endpoint-url http://rustfs:9000 2>/dev/null \
	|| docker run --rm \
		--network $(NETWORK_NAME) \
		-e AWS_ACCESS_KEY_ID=$(RUSTFS_ACCESS_KEY) \
		-e AWS_SECRET_ACCESS_KEY=$(RUSTFS_SECRET_KEY) \
		-e AWS_DEFAULT_REGION=us-east-1 \
		amazon/aws-cli:latest \
		s3api create-bucket \
		--bucket $(ICEBERG_WAREHOUSE_BUCKET) \
		--endpoint-url http://rustfs:9000
	@printf "$(GREEN)✔  Bucket '$(ICEBERG_WAREHOUSE_BUCKET)' is ready.$(RESET)\n"

# ── Notebook curriculum helpers ────────────────────────────────────
jupyter-rebuild:
	@printf "$(BOLD)$(CYAN)▶  Rebuilding Jupyter image [instance: $(INSTANCE)]...$(RESET)\n"
	@$(COMPOSE_LOCAL) build jupyter
	@$(COMPOSE_LOCAL) up -d jupyter
	@printf "$(GREEN)✔  Jupyter rebuilt and restarted.$(RESET)\n"
	@printf "$(DIM)   Tip: PySpark is a 317 MB download. If it times out, just rerun — layers above it are cached.$(RESET)\n"

reset-events-table:
	@printf "$(BOLD)$(YELLOW)▶  Dropping demo.events and pruning warehouse prefix [instance: $(INSTANCE)]...$(RESET)\n"
	@$(COMPOSE_LOCAL) exec -T jupyter python < scripts/reset_events_table.py
	@docker run --rm \
		--network $(NETWORK_NAME) \
		-e AWS_ACCESS_KEY_ID=$(RUSTFS_ACCESS_KEY) \
		-e AWS_SECRET_ACCESS_KEY=$(RUSTFS_SECRET_KEY) \
		-e AWS_DEFAULT_REGION=us-east-1 \
		amazon/aws-cli:latest \
		s3 rm s3://$(ICEBERG_WAREHOUSE_BUCKET)/warehouse/demo/ \
		--recursive \
		--endpoint-url http://rustfs:9000 2>/dev/null || true
	@printf "$(GREEN)✔  demo.events reset. Re-run notebook 00 then 01.$(RESET)\n"

reset-nessie:
	@printf "$(BOLD)$(RED)⚠  Wiping all Nessie state for instance '$(INSTANCE)' — every catalog ref, branch, and tag will be lost.$(RESET)\n"
	@printf "$(RED)   Press Ctrl-C within 5s to abort...$(RESET)\n"
	@sleep 5
	@$(COMPOSE_LOCAL) rm -sf nessie
	@docker volume rm $(PROJECT_NAME)_nessie-sn-data 2>/dev/null || true
	@$(COMPOSE_LOCAL) up -d nessie
	@printf "$(YELLOW)   Waiting for Nessie health...$(RESET)\n"
	@sleep 10
	@$(MAKE) nessie-init-bucket
	@printf "$(GREEN)✔  Nessie reset complete.$(RESET)\n"

# ── SSL setup ──────────────────────────────────────────────────────
setup-certs:
	@printf "$(BOLD)$(CYAN)▶  Generating SSL certificates for RustFS...$(RESET)\n"
	@bash scripts/setup-certs.sh
