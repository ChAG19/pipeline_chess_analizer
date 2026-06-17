import json, yaml, io

class FileReader:
    def __init__(self):
         pass

    #Принимает название yaml файла (без .yaml)
    @staticmethod
    def readYamlRule(rule_file_name):
        with open ("config\\" + rule_file_name + '.yaml', 'r', encoding='utf-8') as r:
            rules = yaml.safe_load(r)
        return rules

    #Принимает строку
    #Возвращает json объект
    @staticmethod
    def readJsonFile(file):
        data = []
        data.append(json.loads(file))
        return data
    
    #Принимает строку
    #Возвращает массив json объектов
    @staticmethod
    def readNdjsonFile(file: str):
        json_rows = []
        for line in file.strip().splitlines():
                if line:
                    json_rows.append(json.loads(file))
        return json_rows