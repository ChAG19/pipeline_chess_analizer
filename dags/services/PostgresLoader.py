from airflow.providers.postgres.hooks.postgres import PostgresHook

class PostgresLoader:
    def __init__(self, postgres_conn) -> None:
        self.hook = PostgresHook(aws_conn_id=postgres_conn)

    def sendDataFromFileToPostgres(self, source, target, rule):
        data = rule(source)

        self.hook

    def parseJsonFile(self):
        pass