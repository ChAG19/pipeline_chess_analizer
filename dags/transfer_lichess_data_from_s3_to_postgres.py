import textwrap, requests, logging
from pendulum import DateTime
from datetime import datetime, timedelta

from airflow.models.variable import Variable
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException

from services.MinioLoader import MinioLoader

# Конфигурация
OWNER = "AG.Chumachenko"
DAG_ID = "transfer_lichess_data_from_s3_to_postgres"

# Таблицы
LAYER_POSTGRES = "ods"
LAYER_S3 = "raw"
SOURCE = "lichess.com" 
BUCKET = "prod"


#Minio ключи
MINIO_CONNECTION = "minio_default"

# Описание
LONG_DESCRIPTION = """
LONG_DESCRIPTION
"""

SHORT_DESCRIPTION = "Загрузка данных Lichess из S3 в postgres"

# Пользователь для анализа
USERNAME = Variable.get("chess_com_username")

args = {
    "owner": OWNER,
    "catchup":True,
    "retries":2,
    "retry_delay": timedelta(minutes=1)
}

# Функция выбора названия файла для загрузки
def get_file_name(conn: MinioLoader, file_path) -> str:
    names = conn.get_files_list(file_path)
    return names[0]
   
@dag(
    default_args=args,
    dag_id=DAG_ID,
    description=SHORT_DESCRIPTION,
    schedule=timedelta(days=1),
    start_date=datetime(2026, 3, 16),
    catchup=False,
    concurrency=1,
    max_active_runs=1,
    max_active_tasks=1, 
    tags=["ods", "s3", "lichess", "psa"],
)

def transfer_lichess_data_from_s3_to_postgres():

    # Функция возвращает файл из s3
    @task()
    def get_file_from_s3(**context):

        conn = MinioLoader(minio_conn=MINIO_CONNECTION, bucket_name=BUCKET)
        """
        loading_date: DateTime = context["logical_date"]
        file_name = loading_date.format("DwwD_MM_YYYY_HH_mm_ss") + ".json"
        file_path = f"{LAYER}/{SOURCE}/{year}/{month}/{file_name}"
        """
        file_path = f"{BUCKET}/{LAYER_S3}/{SOURCE}/{context["data_interval_start"].format("DD_MM_YYYY_HH")}"

        logging.info("Начат поиск файла")
        file_name = get_file_name(conn, file_path)
        if file_name is not None:
            logging.info("Найден файл {file_name}")
            
            logging.info("Начало загрузки файла в s3 хранилище")
            try:
                file = conn.download(file_path=file_name)
            except Exception as e:
                raise AirflowException("Ошибка соединения")
            
            logging.info("Файл получен")

        else:
            file = None
            logging.info("Файл не найден")
        
        return file
    
    def transform_data_to_postgres_form(file):
        pass
    
    
    @task.run_if(lambda context: context["task_instance"].xcom_pull(task_ids='get_file_from_s3'), 
                 skip_message="Данных за этот месяц нет, загрузка пропущена")

    @task
    def load_s3_data_to_postgres(file, **context):
        file = transform_data_to_postgres_form(file)
        pass

transfer_lichess_data_from_s3_to_postgres()

