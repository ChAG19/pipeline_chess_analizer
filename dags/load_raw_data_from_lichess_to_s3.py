import textwrap, requests, logging
from pendulum.datetime import DateTime
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.models.variable import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Конфигурация
OWNER = "AG.Chumachenko"
DAG_ID = "load_raw_data_from_lichess_to_s3"

# Таблицы
LAYER = "raw"
SOURCE = "lichess.com" 

#Minio ключи
ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

# Описание
LONG_DESCRIPTION = """
LONG_DESCRIPTION
"""

SHORT_DESCRIPTION = "SHORT_DESCRIPTION"

# Пользователь для анализа
USERNAME = Variable.get("username")
args = {
    "owner": OWNER,
    "catchup":True,
    "retries":2,
    "retry_delay": timedelta(minutes=1)
}

def load_raw_data_from_lichess_to_s3(**context):
    return

def get_file_from_lichess(**context):
    start, end = get_loading_time(**context)
    logging.info(f"Запущена загрузка данных за {start.date()}")
    
    response = requests.get(
        url=f"https://lichess.org/api/games/user/{USERNAME}", 
        params={'since': start.int_timestamp*1000, "until": end.int_timestamp*1000} # Умножение на 1000 нужно для нужного формата для Lichess
        )
    
    if response.ok:
        logging.info("Данные за {start} получены")
        return response.text
    logging.info("Ошибка соединения. Данные не получены!")
    return None

# Получение диапозона загрузки
# Вход: data_interval_end (внутри **context)
# Выход: начальная и конечная дата
def get_loading_time(**context) -> tuple[DateTime, DateTime]: 
    end_timestamp = context["data_interval_end"]
    end_timestamp = end_timestamp.subtract(days=1)
    start_timestamp = end_timestamp
    end_timestamp = end_timestamp.end_of('day')
    start_timestamp = start_timestamp.start_of('day')
    return start_timestamp, end_timestamp

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
    tags=["raw", "s3", "lichess"],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = EmptyOperator(
        task_id = "start"
    )

    t2 = BashOperator(
        task_id="date",
        bash_command="date",
    )
    t1.doc_md = textwrap.dedent(
        """\
    #### Task Documentation
    You can document your task using the attributes `doc_md` (markdown),
    `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    rendered in the UI's Task Instance Details page.
    ![img](https://imgs.xkcd.com/comics/fixing_problems.png)
    **Image Credit:** Randall Munroe, [XKCD](https://xkcd.com/license.html)
    """
    )

    dag.doc_md = __doc__  # providing that you have a docstring at the beginning of the DAG; OR
    dag.doc_md = """
    This is a documentation placed anywhere
    """  # otherwise, type it like this
    templated_command = textwrap.dedent(
        """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
    {% endfor %}
    """
    )

    t3 = EmptyOperator(
        task_id="End",
    )

    t1 >> t2 >> t3