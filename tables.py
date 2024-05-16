import os
from os import path
import shutil
from migrate import Handle_Data_Types
from typing import Dict, Any, List
import json


class Tables():

    def __init__(self):
        self._table: None | Dict[str, Any] = None
        self._translate_dict: None | Dict[str, Any] = None
        self._handle_data_types: Handle_Data_Types = Handle_Data_Types()
        self._data_types: None | Dict[str, Any] = None

    @property
    def table(self):
        return self._table

    @property
    def translate_dict(self):
        return self._translate_dict

    @property
    def data_types(self):
        return self._data_types

    def set_original_data(self, id: str, data: Dict[str, Any]):        
        self.save_on_db(id, 'original_data.json', data)
        self._table = data

    def save(self, db_directory: str, db_path: str, data: str | Dict[str, Any]):
        exists = path.exists(db_directory)
        if not exists:
            os.makedirs(db_directory, exist_ok=True)
        with open(db_path, 'w') as file:
            if isinstance(data, str):
                file.write(data)
            else:
                json.dump(data, file, indent=4)

    def get_table(self, id: str):
        json_path = path.join(path.dirname(__file__),
                              "db", id, "original_data.json")
        exists = path.exists(json_path)
        if exists:
            with open(json_path, "r", encoding='utf-8') as opened:
                json_content = json.load(opened)
            self._table = json_content

            return json_content
        return None

    def _transtale_with_key_replace(self, data: Dict[str, Any], translation_dict: Dict[str, Any]):
        translated = {translation_dict.get(
            key, key): value for key, value in data.items()}
        return translated

    def _translate_with_value_replace(self, data: Dict[str, Any], translation_dict: Dict[str, str]):
        old_key_and_new_key = {
            key: translation_dict[key] for key in data.keys()}
        return old_key_and_new_key

    def translate(self, data: Dict[str, Any], translation_dict: Dict[str, str], dict_id: str):
        translated = self._transtale_with_key_replace(data, translation_dict)
        old_key_new_key = self._translate_with_value_replace(
            data, translation_dict)
        
        self.save_on_db(dict_id, "translated.json", translated)
        self.save_on_db(dict_id, "old_key_new_key.json", old_key_new_key)        

        self._translate_dict = old_key_new_key

        return {'translated': translated, 'old_key_new_key': old_key_new_key}

    def get_translate_dict(self, id: str):
        json_path = path.join(path.dirname(__file__),
                              "db", id, "old_key_new_key.json")
        exists = path.exists(json_path)
        if exists:
            with open(json_path, "r", encoding='utf-8') as opened:
                json_content = json.load(opened)
            self._translate_dict = json_content

            return json_content
        return None

    def handle_data_types(self, main_dict: Dict[str, Any], raw_datatypes: str, translate_obj: Dict[str, str] | None, dict_id: str):
        datatypes = Handle_Data_Types()
        datatypes_dict = datatypes.make_dict(raw_datatypes)

        handle_data = self._handle_data_types
        transformed_data = handle_data.migrate(
            main_dict, datatypes_dict, translate_obj)

        self.save_on_db(dict_id, "raw_datatypes.txt", raw_datatypes)

        translate_dict = self.get_translate_dict(dict_id)
        if bool(translate_dict):
            translated = self._transtale_with_key_replace(
                transformed_data, translate_dict)

            self.save_on_db(dict_id, "datatypes.json", translated)

            self._data_types = translated

    def save_on_db(self, dict_id: str, file_name: str, data: str | Dict[str, Any]):
        db_directory = path.join(path.dirname(__file__), "db", dict_id)
        dt_path = path.join(db_directory, file_name)
        self.save(db_directory, dt_path, data)

    def get_datatypes(self, id: str):
        json_path = path.join(path.dirname(__file__),
                              "db", id, "datatypes.json")
        exists = path.exists(json_path)
        if exists:
            with open(json_path, "r", encoding='utf-8') as opened:
                json_content = json.load(opened)
            self._data_types = json_content

            return json_content
        return None
 
    def sort_by_primary_key(self, data: Dict[str, Any], pks: List[str]):
        primary_keys = {key: value for key, value in data.items() if key in pks}
        not_primary_keys = {key: value for key, value in data.items() if key not in pks}

        return {**primary_keys, **not_primary_keys}
    
    def make_full_dict(self, source_table_name:str, destiny_table_name:str, schema:str, pks:List[str], sort_keys:List[str]):
        data = {
            destiny_table_name: {
                'tabela_origem': source_table_name,
                'tabela_destino': destiny_table_name,
                'ovs': schema,
                'keys': [self.translate_dict[key] for key in pks],
                'sort_keys': [self.translate_dict[key] for key in sort_keys],
                'columns': self.translate_dict,
                'columns_format': self.data_types                
            }
        }
        return data

    def load(self, id: str, data: Dict[str, Any], translation_dict: Dict[str, str], datatypes_dict: Dict[str, Any], source_table_name: str, destiny_table_name: str, schema: str, pks: List[str], sort_keys: List[str], translate_obj: Dict[str, str] | None):
        self.set_original_data(id, data)
        sort_pks = self.sort_by_primary_key(data, [*pks, *sort_keys])
        self.translate(sort_pks, translation_dict, id)        
        self.handle_data_types(sort_pks, datatypes_dict, translate_obj, id)

        full_dict = self.make_full_dict(source_table_name, destiny_table_name, schema, pks, sort_keys)
        self.save_on_db(id, 'full_dict.json', full_dict)

        print(f"table --> {json.dumps(self.table, indent=4)}")
        print(
            f"translate_dict --> {json.dumps(self.translate_dict, indent=4)}")
        print(f"datatypes --> {json.dumps(self.data_types, indent=4)}")

    def delete(self, id: str):
        json_path = path.join(path.dirname(__file__), "db", id)
        exists = path.exists(json_path)
        if exists:
            shutil.rmtree(json_path)
            print(f"Table {id} deleted!")
            return

        print("Table not found")
