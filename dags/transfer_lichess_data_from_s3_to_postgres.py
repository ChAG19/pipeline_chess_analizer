import textwrap, requests, logging
from pendulum import DateTime
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.models.variable import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from services.MinioLoader import MinioLoader

# Конфигурация
OWNER = "AG.Chumachenko"
DAG_ID = "transfer_lichess_data_from_s3_to_postgres"

# Таблицы
LAYER = "ods"
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

# Функция отправляет файл в s3
def get_file_from_s3(**context):

    conn = MinioLoader(minio_conn=MINIO_CONNECTION, bucket_name=BUCKET)
    """
    loading_date: DateTime = context["logical_date"]
    file_name = loading_date.format("DD_MM_YYYY_HH_mm_ss") + ".json"
    file_path = f"{LAYER}/{SOURCE}/{year}/{month}/{file_name}"
    """

    logging.info("Начало загрузки файла в s3 хранилище")
    file = conn.download(file_path="raw/lichess.com/19_04_2026/19_04_2026_00_00_00.json")
    
    return file

    
# Получение файла с партиями
# Вход: месяц загрузки(int), год загрузки(int), **context
# Выход: полученный файл виде строки или None
def get_file_from_chess_com(month, year, **context):
    logging.info(f"Запущена загрузка данных за {month} {year}")
    
    try:
        response = requests.get(
            url=f"https://api.chess.com/pub/player/{USERNAME}/games/{year}/{month}",
            headers = {
                "User-Agent": f"pipeline_chess_analizer/0.1.0 (contact: {USERNAME})"
                } #требование  chess.com
        )
    except requests.exceptions.RequestException as e:
        logging.info("Ошибка соединения")
    
    if response.ok:
        logging.info(f"Данные за {month} {year} получены.")
        return response.text
    
    logging.info("Ошибка. Данные не получены!")
    return None

# Получение месяца и года загрузки, преобразование месяца в формат mm
# Вход: data_interval_end (внутри **context)
# Выход: месяц(str) и год(int)
def get_loading_time(**context) -> tuple[str, int]: 
    date: DateTime = context["data_interval_end"]
    return  f"{date.month:02d}"  , date.year

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

    t2 = PythonOperator(
        task_id="load_raw_data_from_lichess_to_s3",
        python_callable=transfer_lichess_data_from_s3_to_postgres
    )
    t2.doc_md = textwrap.dedent(
        """\
    #### Запуск Dag
    Основной шаг, который скачивает и обрабатывает lichess файл из Minio хранилища в Postgres
    """
    )

    t3 = EmptyOperator(
        task_id="End",
    )
    t3.doc_md = textwrap.dedent(
        """
    #### Запуск Dag
    Окончание дага
    """
    )

    t1 >> t2 >> t3