import textwrap, requests, logging
from pendulum import DateTime
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.models.variable import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

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

# Основная функция, инициализирует загрузку и передачу данных
def transfer_lichess_data_from_s3_to_postgres(**context):
    file = get_file_from_s3(**context)

    if file is not None:
        logging.info(f"Файл загружен")

    else: 
        logging.info(f"Файл не загружен") 

# Функция возвращает файл из s3
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
        file = conn.download(file_path=file_name)
    else:
        file = None
        logging.info("Файл не найден")
    
    return file

def get_file_name(conn: MinioLoader, file_path) -> str:
    names = conn.get_files_list(file_path)
    return names[0]

with DAG(
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
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = EmptyOperator(
        task_id = "Start"
    )
    t1.doc_md = textwrap.dedent(
        """
    #### Запуск Dag
    Запуск дага
    """
    )
    t2 = ExternalTaskSensor(
        task_id="wait_for_loading_lichess_data",
        external_dag_id="load_raw_data_from_lichess_to_s3",
        execution_delta=timedelta(0),
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="reschedule",
        timeout=600
    )
    t2.doc_md = textwrap.dedent(
        """
    #### Запуск Dag
    Ожидание загрузки данных в S3
    """
    )

    t3 = PythonOperator(
        task_id="transfer_lichess_data_from_s3_to_postgres",
        python_callable=transfer_lichess_data_from_s3_to_postgres
    )
    t3.doc_md = textwrap.dedent(
        """\
    #### Запуск Dag
    Основной шаг, который скачивает и обрабатывает lichess файл из Minio хранилища в Postgres
    """
    )

    t4 = EmptyOperator(
        task_id="End",
    )
    t4.doc_md = textwrap.dedent(
        """
    #### Запуск Dag
    Окончание дага
    """
    )

    t1 >> t2 >> t3 >> t4