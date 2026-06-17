from airflow.providers.postgres.hooks.postgres import PostgresHook
from services.FileReader import FileReader

class PostgresLoader:
    def __init__(self, postgres_conn) -> None:
        self.hook = PostgresHook(aws_conn_id=postgres_conn)
    #soure - type of file
    def sendDataFromFileToPostgres(self, source, target, rule, data):
        mapping = FileReader.readYamlRule(rule)
        
        if source == "json":
            file_data = FileReader.readJsonFile(data)
        elif source == "ndjson":
            file_data = FileReader.readNdjsonFile(data)

            #db_colunms = [m['db_colunm'] for m in mapping['mappings']]

        transformed_data = []

        for line in file_data:
            for row in data:
                row_values = []
                for mapping in  mapping['mappings']:
                   pass 
        cursor = self.hook.get_conn().cursor()
        cursor.execute("""
COPY 
""")

  
