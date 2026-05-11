import textwrap, requests, logging
from pendulum import DateTime
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.models.variable import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task

from services.MinioLoader import MinioLoader

# Конфигурация
OWNER = "AG.Chumachenko"
DAG_ID = "load_raw_data_from_lichess_to_s3"

# Таблицы
LAYER = "raw"
SOURCE = "lichess.com" 
BUCKET = "prod"

#Minio ключи
MINIO_CONNECTION = "minio_default"

# Описание
LONG_DESCRIPTION = """
LONG_DESCRIPTION
"""

SHORT_DESCRIPTION = "Загрузка данных lichess.com в s3"

# Пользователь для анализа
USERNAME = Variable.get("lichess_username")

args = {
    "owner": OWNER,
    "catchup":True,
    "retries":2,
    "retry_delay": timedelta(minutes=1)
}

# Получение диапозона загрузки
# Вход: data_interval_end (внутри **context)
# Выход: начальная и конечная дата
def get_loading_time(**context) -> tuple[DateTime, DateTime]: 
    end_timestamp: DateTime = context["data_interval_end"]
    end_timestamp = end_timestamp.subtract(days=1)
    start_timestamp = end_timestamp
    end_timestamp = end_timestamp.end_of('day')
    start_timestamp = start_timestamp.start_of('day')
    return start_timestamp, end_timestamp

@dag(
    default_args=args,
    dag_id=DAG_ID,
    description=SHORT_DESCRIPTION,
    schedule="@daily",
    start_date=datetime(2026, 5, 9),
    catchup=False,
    concurrency=1,
    max_active_runs=1,
    max_active_tasks=1, 
    tags=["raw", "s3", "lichess"]
)

def load_raw_data_from_lichess_to_s3():
    # Получение файла с партиями
    # Вход: **context
    # Выход: полученный файл виде строки или None
    @task()
    def get_file_from_lichess(**context):
        start, end = get_loading_time(**context)
        
        logging.info(f"Запущена загрузка данных за {start.date()} из {SOURCE}")
        
        try:
            response = requests.get(
                url=f"https://lichess.org/api/games/user/{USERNAME}", 
                params={'since': start.int_timestamp*1000, "until": end.int_timestamp*1000, 'opening':'true'}, # Умножение на 1000 нужно для нужного формата для Lichess
                headers={'Accept':'application/x-ndjson'}
            )
        except requests.exceptions.RequestException as e:
            logging.info("Ошибка соединения")
        
        logging.info(f"Данные за {start.date()} получены.")
        return response.text
        
    
    @task.run_if(lambda chess_data: len(chess_data) != 0)
    
    @task()
    # Функция отправляет файл в s3
    def load_data_to_s3(file, **context):
        date: DateTime = context["data_interval_start"]
        conn = MinioLoader(minio_conn=MINIO_CONNECTION, bucket_name=BUCKET)

        loading_date: DateTime = context["logical_date"]
        file_name = loading_date.format("DD_MM_YYYY_HH_mm_ss") + ".json"
        file_path = f"{LAYER}/{SOURCE}/{date.format("DD_MM_YYYY")}/{file_name}"

        logging.info("Начало загрузки файла в s3 хранилище")
        conn.upload(file=file, file_path=file_path)
        logging.info(f"Файл {file_name} загружен\n{file_path}")

    file = get_file_from_lichess()
    load_data_to_s3(file)

load_raw_data_from_lichess_to_s3()