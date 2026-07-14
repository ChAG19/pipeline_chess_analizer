import json, yaml, io
import pandas as pd

class FileReader:
    def __init__(self):
         pass

    #Принимает название yaml файла (без .yaml)
    @staticmethod
    def readYamlRule(rule_file_name):
        
        with open ("config/" + rule_file_name + '.yaml', 'r', encoding='utf-8') as r:
            rules = yaml.safe_load(r)
        return rules['mappings']

    #Принимает строку
    #Возвращает DataFrame
    @staticmethod
    def readJsonFile(file):
        return pd.DataFrame([json.loads(file)])
    
    #Принимает строку
    #Возвращает DataFrame
    @staticmethod
    def readNdjsonFile(file: str):
        # Считываем json
        json_rows = []
        for line in file.strip().splitlines():
                line = line.strip()
                if line:
                    json_rows.append(json.loads(line))
        
        # Преобразуем в DataFrame
        data = pd.DataFrame()

        for line in json_rows:
            if line:
                data = pd.concat([data, pd.json_normalize(line)])
        return data