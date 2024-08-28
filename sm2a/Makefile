SECRET_NAME=""
ENV_FILE=".env"


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
	docker compose run --rm airflow-cli db init
	docker compose run --rm airflow-cli users create --email airflow@example.com --firstname airflow --lastname airflow --password airflow --username airflow --role Admin

sm2a-local-stop:
	docker compose down


sm2a-deploy:
	@echo "Installing the deployment dependency"
	pip install -r ../deploy_requirements.txt
	@echo "Deploying SM2A"
	cp -r ../dags .
	python scripts/generate_env_file.py --secret-id ${SECRET_NAME} --env-file ${ENV_FILE}
	./scripts/deploy.sh ${ENV_FILE} <<< init
	./scripts/deploy.sh ${ENV_FILE} <<< deploy

clean: sm2a-local-stop
	@echo "Cleaning local env"
	docker container prune -f
	docker image prune -f
	docker volume prune -f
