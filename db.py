import os
from os import path
import shutil
from typing import Dict, Any
import json
from tables import Tables


class DB():

    def __init__(self, parent: Tables) -> None:
        self.parent = parent

    def save(self, db_directory: str, db_path: str, data: str | Dict[str, Any]):
        exists = path.exists(db_directory)
        if not exists:
            os.makedirs(db_directory, exist_ok=True)
        with open(db_path, 'w') as file:
            if isinstance(data, str):
                file.write(data)
            else:
                json.dump(data, file, indent=4)

    def save_on_db(self, dict_id: str, file_name: str, data: str | Dict[str, Any]):
        db_directory = path.join(path.dirname(__file__), "db", dict_id)
        dt_path = path.join(db_directory, file_name)
        self.save(db_directory, dt_path, data)

    def get_translate_dict(self, id: str):
        json_path = path.join(path.dirname(__file__),
                              "db", id, "old_key_new_key.json")
        exists = path.exists(json_path)
        if exists:
            with open(json_path, "r", encoding='utf-8') as opened:
                json_content = json.load(opened)
            self.parent._translate_dict = json_content

            return json_content
        return None

    def get_datatypes(self, id: str):
        json_path = path.join(path.dirname(__file__),
                              "db", id, "datatypes.json")
        exists = path.exists(json_path)
        if exists:
            with open(json_path, "r", encoding='utf-8') as opened:
                json_content = json.load(opened)
            self.parent._data_types = json_content

            return json_content
        return None

    def set_original_data(self, id: str, data: Dict[str, Any]): 
        self.save_on_db(id, 'original_data.json', data)
        self.parent._table = data

    def get_table(self, id: str):
        json_path = path.join(path.dirname(__file__),
                              "db", id, "original_data.json")
        exists = path.exists(json_path)
        if exists:
            with open(json_path, "r", encoding='utf-8') as opened:
                json_content = json.load(opened)
            self.parent._table = json_content

            return json_content
        return None

    def delete(self, id: str):
        json_path = path.join(path.dirname(__file__), "db", id)
        exists = path.exists(json_path)
        if exists:
            shutil.rmtree(json_path)
            print(f"Table {id} deleted!")
            return

        print("Table not found")
