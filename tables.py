import os
from os import path
from migrate import Handle_Data_Types
from typing import Dict, Any
import json

class Tables():

    def __init__(self):
        self._table:None | Dict[str,Any] = None
        self._translate_dict:None | Dict[str,Any] = None
        self._handle_data_types:Handle_Data_Types = Handle_Data_Types()
        self._data_types:None | Dict[str,Any] = None
        
    @property
    def table(self):
        return self._table
    
    @property
    def translate_dict(self):
        return self._translate_dict
    
    @property
    def data_types(self):
        return self._data_types
    
    def set_original_data(self, id:str, data:Dict[str,Any]):
        db_directory = path.join(path.dirname(__file__),"db", id)
        db_path = path.join(db_directory, "original_data.json")
        self.save(db_directory, db_path, data)
        self._table = data

    def save(self, db_directory:str, db_path:str, data:Dict[str,Any]):
        exists = path.exists(db_directory)
        if not exists:
            os.makedirs(db_directory, exist_ok=True)
        with open(db_path, 'w') as file:
            json.dump(data, file, indent=4)

    def get_table(self, id:str):
        json_path = path.join(path.dirname(__file__),"db", id, "original_data.json")
        exists = path.exists(json_path)
        if exists:
            with open(json_path, "r", encoding='utf-8') as opened:
                json_content = json.load(opened)
            self._table = json_content
            
            return json_content
        return None
    
    def _transtale_with_key_replace(self, data:Dict[str,Any], translation_dict:Dict[str,Any]):
        translated = {translation_dict.get(key,key):value for key, value in data.items()}
        return translated

    def _translate_with_value_replace(self, data:Dict[str,Any], translation_dict:Dict[str,str]):
        old_key_and_new_key = {key:translation_dict[key] for key in data.keys()}
        return old_key_and_new_key
    
    def translate(self, data:Dict[str,Any], translation_dict:Dict[str,str], dict_id:str):
        translated = self._transtale_with_key_replace(data, translation_dict)
        old_key_new_key = self._translate_with_value_replace(data, translation_dict)

        db_directory = path.join(path.dirname(__file__),"db", dict_id)
        translated_dir = path.join(db_directory, "translated.json")        
        old_key_new_key_dir = path.join(db_directory, "old_key_new_key.json")

        self.save(db_directory, translated_dir, translated)        
        self.save(db_directory, old_key_new_key_dir, old_key_new_key)

        self._translate_dict = old_key_new_key

        return { 'translated':translated, 'old_key_new_key':old_key_new_key}    

    def get_translate_dict(self, id:str):
        json_path = path.join(path.dirname(__file__),"db", id, "old_key_new_key.json")
        exists = path.exists(json_path)
        if exists:
            with open(json_path, "r", encoding='utf-8') as opened:
                json_content = json.load(opened)
            self._translate_dict = json_content
            
            return json_content
        return None
    
    def handle_data_types(self, main_dict:Dict[str,Any], datatypes_dict:Dict[str,Any], translate_obj:Dict[str,str] | None, dict_id:str):
        handle_data = self._handle_data_types
        transformed_data = handle_data.migrate(main_dict, datatypes_dict, translate_obj)

        translate_dict = self.get_translate_dict(dict_id)
        if bool(translate_dict):
            translated = self._transtale_with_key_replace(transformed_data, translate_dict)

            db_directory = path.join(path.dirname(__file__),"db", dict_id)
            db_path = path.join(db_directory, "datatypes.json")
            self.save(db_directory, db_path, translated)
            self._data_types = translated
    
    def get_datatypes(self, id:str):
        json_path = path.join(path.dirname(__file__),"db", id, "datatypes.json")
        exists = path.exists(json_path)
        if exists:
            with open(json_path, "r", encoding='utf-8') as opened:
                json_content = json.load(opened)
            self._data_types = json_content
            
            return json_content
        return None
    
    def load(self, id:str, data:Dict[str,Any], translation_dict:Dict[str,str], datatypes_dict:Dict[str,Any], translate_obj:Dict[str,str] | None):
        self.set_original_data(id, data)
        self.translate(data, translation_dict, id)
        self.handle_data_types(data, datatypes_dict, translate_obj, id)

        print(f"table --> {json.dumps(self.table, indent=4)}")
        print(f"translate_dict --> {json.dumps(self.translate_dict, indent=4)}")
        print(f"datatypes --> {json.dumps(self.data_types, indent=4)}")
    
    def delete(self, id:str):
        json_path = path.join(path.dirname(__file__),"db", id)
        exists = path.exists(json_path)
        if exists:
            os.remove(json_path)
            print(f"Table {id} deleted!")
            return

        print("Table not found")
