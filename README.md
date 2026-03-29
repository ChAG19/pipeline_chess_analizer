Readme
python -m venv venv - создать виртуальное окружение
venv\\Scripts\\activate - запуск виртуального окружения
docker-compose down -v - остановить контейнеры докер
docker-compose up -d  - запустить контейнеры докер


Версии:
Airflow - 2.10.5
apache-airflow-providers-amazon==9.17.0 (9.2.0)           

HOW TO:
1) Первый запуск

- В airflow ui прописать переменную lichess_user
- Создать соединение в Connections.