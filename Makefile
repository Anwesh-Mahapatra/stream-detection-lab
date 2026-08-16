.PHONY: up down topics logs submit test clean help

COMPOSE := docker compose -f docker/docker-compose.yml --env-file docker/.env

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*##' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*##"}; {printf "%-10s %s\n", $$1, $$2}'

up: ## Start kafka, flink, elasticsearch, kibana (docker/.env must exist - run scripts/bootstrap.sh first)
	$(COMPOSE) up -d --build

down: ## Stop all services, keep volumes (data survives)
	$(COMPOSE) down

topics: ## Create k8s-audit-raw and k8s-audit-ecs (idempotent)
	./scripts/create-topics.sh

logs: ## Tail logs from every service
	$(COMPOSE) logs -f --tail=100

submit: ## Submit pipeline/jobs/k8s_audit_job.py to the Flink cluster
	$(COMPOSE) exec jobmanager flink run -py /opt/sdl/pipeline/jobs/k8s_audit_job.py --pyFiles /opt/sdl

test: ## Run pytest locally (needs: pip install -r requirements.txt)
	python3 -m pytest tests/ -v

clean: ## Stop all services and delete volumes - destroys Kafka/ES data, not reversible
	$(COMPOSE) down -v
