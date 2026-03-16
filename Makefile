.PHONY: help setup lint test format infra-plan infra-apply

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

setup: ## Install all dependencies
	pip install -r requirements.txt
	pre-commit install

lint: ## Run linting
	ruff check .
	sqlfluff lint transformation/

format: ## Auto-format code
	ruff format .
	sqlfluff fix transformation/

test-unit: ## Run unit tests
	pytest tests/unit/ -v

test-integration: ## Run integration tests
	pytest tests/integration/ -v

infra-plan-gcp: ## Terraform plan for GCP
	cd infrastructure/terraform/gcp && terraform plan

infra-apply-gcp: ## Terraform apply for GCP
	cd infrastructure/terraform/gcp && terraform apply

dbt-debug: ## Test dbt connection
	cd transformation && dbt debug

dbt-run: ## Run dbt models
	cd transformation && dbt run

dbt-test: ## Run dbt tests
	cd transformation && dbt test

dbt-docs: ## Generate and serve dbt docs
	cd transformation && dbt docs generate && dbt docs serve

airflow-up: ## Start Airflow locally
	docker compose -f infrastructure/docker/docker-compose-airflow.yml up -d

airflow-down: ## Stop Airflow locally
	docker compose -f infrastructure/docker/docker-compose-airflow.yml down
