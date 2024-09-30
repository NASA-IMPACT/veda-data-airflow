SECRET_NAME=""
ENV_FILE=".env"
GITHUB_ACTIONS_ENV := $(shell test -n "$$GITHUB_ACTIONS" && echo "true" || echo "false")

important_message = \
	@echo "\033[0;31m$(1) \033[0m"

info_message = \
	@echo "\033[0;32m$(1) \033[0m"

count_down = \
	@echo "Spinning up the system please wait..."; \
	secs=40 ;\
	while [ $$secs -gt 0 ]; do \
		printf "%d\033[0K\r" $$secs; \
		sleep 1; \
		: $$((secs--)); \
	done;

.PHONY:
	clean
	all
	list

all: sm2a-local-init sm2a-local-run

sm2a-local-run: sm2a-local-stop
	@echo "Running SM2A"
	docker compose up -d
	$(call important_message, "Give the resources a minute to be healthy 💪")
	$(count_down)
	$(call info_message, "Please visit http://localhost:8080")
	@echo "username:airflow | password:airflow"
	@echo "To use local SM2A with AWS update ${SM2A_FOLDER}/sm2a-local-config/.env AWS credentials"

sm2a-local-init:
	cp sm2a-local-config/env_example sm2a-local-config/.env
	cp -r ../dags .
	docker compose run --rm airflow-cli db init
	docker compose run --rm airflow-cli users create --email airflow@example.com --firstname airflow --lastname airflow --password airflow --username airflow --role Admin
	rm -rf dags

sm2a-local-stop:
	docker compose down

sm2a-local-build:
	cp -r ../dags .
	docker compose build
	rm -rf dags

sm2a-deploy:
ifeq ($(GITHUB_ACTIONS_ENV),true)
	@echo "Installing the deployment dependency"
	@echo "Deploying SM2A"
	python scripts/generate_env_file.py --secret-id ${SECRET_NAME} --env-file ${ENV_FILE}
	@bash -c './scripts/deploy.sh ${ENV_FILE} <<< init'
	@bash -c './scripts/deploy.sh ${ENV_FILE} <<< deploy'
else
	$(call important_message, "Wait a minute you are not github 😡")
endif

clean: sm2a-local-stop
	@echo "Cleaning local env"
	docker container prune -f
	docker image prune -f
	docker volume prune -f

list:
	@grep '^[^#[:space:]].*:' Makefile
