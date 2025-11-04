# IDS706_DE_Wk10_Airflow_pipeline

Login into http://localhost:8080/ 
Username:airflow
Password:airflow


Build docker and start

docker compose -f .devcontainer/docker-compose.yml build
docker compose -f .devcontainer/docker-compose.yml up airflow-init
docker compose -f .devcontainer/docker-compose.yml up -d