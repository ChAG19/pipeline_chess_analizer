import textwrap, requests, logging
from pendulum import DateTime
from datetime import datetime, timedelta

from airflow.models.variable import Variable
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException

from services.MinioLoader import MinioLoader

# Конфигурация
OWNER = "AG.Chumachenko"
DAG_ID = "load_raw_data_from_chess_com_to_s3"

# Таблицы
LAYER = "raw"
SOURCE = "chess.com" 
BUCKET = "prod"

#Minio ключи
MINIO_CONNECTION = "minio_default"

# Описание
LONG_DESCRIPTION = """
LONG_DESCRIPTION
"""

SHORT_DESCRIPTION = "Загрузка данных chess.com в s3"

# Пользователь для анализа
USERNAME = Variable.get("chess_com_username")

# Получение месяца и года загрузки, преобразование месяца в формат mm
# Вход: data_interval_end (внутри **context)
# Выход: месяц(str) и год(int)
def get_loading_time(**context) -> tuple[str, int]: 
    date: DateTime = context["data_interval_end"]
    return  f"{date.month:02d}"  , date.year

args = {
    "owner": OWNER,
    "catchup":True,
    "retries":2,
    "retry_delay": timedelta('seconds'==30)
}

@dag(
    default_args=args,
    dag_id=DAG_ID,
    description=SHORT_DESCRIPTION,
    schedule="@daily",
    start_date=datetime(2026, 3, 16),
    concurrency=1,
    max_active_runs=1,
    max_active_tasks=1, 
    tags=["raw", "s3", "chess.com"],
)

def load_raw_data_from_chess_com_to_s3():

    # Получение файла с партиями
    # Вход: месяц загрузки(int), год загрузки(int), **context
    # Выход: полученный файл виде строки или None
    @task()
    def get_file_from_chess_com(**context):
        month, year = get_loading_time(**context)
        logging.info(f"Запущена загрузка данных за {month} {year}")

        try:
            response = requests.get(
                url=f"https://api.chess.com/pub/player/{USERNAME}/games/{year}/{month}",
                headers = {
                    "User-Agent": f"pipeline_chess_analizer/0.1.0 (contact: {USERNAME})"
                    } #требование  chess.com
            )
        except requests.exceptions.RequestException as e:
            raise AirflowException("Ошибка соединения")
        
        logging.info(f"Данные за {month} {year} получены.")
        return response.text

    #Если значение не None, не пустая строка, не 0, не пустой список и т.д. — задача выполняется
    @task.run_if(lambda context: context["task_instance"].xcom_pull(task_ids='get_file_from_chess_com'), 
                 skip_message="Данных за этот месяц нет, загрузка пропущена")

    @task()
    # Функция отправляет файл в s3
    def load_data_to_s3(file, **context):
        month, year = get_loading_time(**context)
        conn = MinioLoader(minio_conn=MINIO_CONNECTION, bucket_name=BUCKET)

        loading_date: DateTime = context["logical_date"]
        file_name = loading_date.format("DD_MM_YYYY_HH_mm_ss") + ".json"
        file_path = f"{LAYER}/{SOURCE}/{year}/{month}/{file_name}"

        logging.info("Начало загрузки файла в s3 хранилище")
        conn.upload(file=file, file_path=file_path)
        logging.info(f"Файл {file_name} загружен\n{file_path}")

    file = get_file_from_chess_com()
    load_data_to_s3(file)

load_raw_data_from_chess_com_to_s3()