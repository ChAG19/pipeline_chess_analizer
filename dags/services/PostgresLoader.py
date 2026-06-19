import pandas as pd
from io import StringIO
from airflow.providers.postgres.hooks.postgres import PostgresHook
from services.FileReader import FileReader
from services.FileDataTransformFunctions import FileDataTransformFunctions

class PostgresLoader:
    def __init__(self, postgres_conn) -> None:
        self.hook = PostgresHook(aws_conn_id=postgres_conn)
    #soure - type of file
    #batch_size - на будущее, для пакетной обработки
    def sendDataFromFileToPostgres(self, source, target, rule, schema, data, batch_size=100):
        #Получаем массив правил
        mapping = FileReader.readYamlRule(rule)
        
        if source == "json":
            file_data = FileReader.readJsonFile(data)
        elif source == "ndjson":
            file_data = FileReader.readNdjsonFile(data)

        for m in mapping:
            #Переименовываем столбцы в соответствии с форматом таблицы бд, или добавляем столбец c дефолтным значением
            if m['json_key'] != None:
                file_data = file_data.rename(columns={m['json_key']:m['db_column']})
            else:
                file_data[m['db_column']] = m['default_value']
            #Применяем правило преобразования, если есть
            if m['transform'] != None:
                file_data = FileDataTransformFunctions().transform(func_name=m['transform'], data=file_data, column=m['db_column'])

        engine = self.hook.get_sqlalchemy_engine()       
        try:
            file_data.to_sql(
                name=target,
                con=engine,
                schema=schema,
                if_exists='append',
                index_label=None,
                chunksize=batch_size,
                method='multi'
            )
        except Exception as e:
            raise TypeError("Возникла ошибка при преобразовании типов данных")
        
        



  
