import pandas as pd

class FileDataTransformFunctions:
    def __init__(self) -> None:
        self.functions = {
            "drop_column": self.drop_column,
        }
    # data - данные в виде таблицы
    # column - столбец
    # row - ряд
    def transform(self, func_name, data=None, column=None, row=None):
        if func_name not in self.functions:
            raise KeyError("Использована несуществующая функция в трансформации\n Невозможно продолжить обработку данных")
        func = self.functions[func_name]
        kwargs = {
            'data': data,
            'column': column,
            'row': row
        }
        return func(**kwargs)

    # Принимает датафрейм и название столюца для удаления
    # Возвращает датафрейм без столбца    
    @staticmethod
    def drop_column(data, column):
        return data.drop(column, axis=1)
    
    @staticmethod
    def convert_unix_ms_to_timestamp(data, column):
        data[column] = pd.to_datetime(data[column], unit='ms', utc=True)
        return data