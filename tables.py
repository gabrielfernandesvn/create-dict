# import os
# import shutil
# from os import path
from migrate import Handle_Data_Types
from typing import Dict, Any, List
import json
import re
from db import DB


class Tables():

    def __init__(self):
        self._table: None | Dict[str, Any] = None
        self._translate_dict: None | Dict[str, Any] = None
        self._handle_data_types: Handle_Data_Types = Handle_Data_Types()
        self._data_types: None | Dict[str, Any] = None
        self.DB = DB(self)

    @property
    def table(self):
        return self._table

    @property
    def translate_dict(self):
        return self._translate_dict

    @property
    def data_types(self):
        return self._data_types


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

        self.DB.save_on_db(dict_id, "translated.json", translated)
        self.DB.save_on_db(dict_id, "old_key_new_key.json", old_key_new_key)        

        self._translate_dict = old_key_new_key

        return {'translated': translated, 'old_key_new_key': old_key_new_key}


    def handle_data_types(self, main_dict: Dict[str, Any], raw_datatypes: str, translate_obj: Dict[str, str] | None, dict_id: str):
        datatypes = Handle_Data_Types()
        datatypes_dict = datatypes.make_dict(raw_datatypes)

        handle_data = self._handle_data_types
        transformed_data = handle_data.migrate(
            main_dict, datatypes_dict, translate_obj)

        self.DB.save_on_db(dict_id, "raw_datatypes.txt", raw_datatypes)

        translate_dict = self.DB.get_translate_dict(dict_id)
        if bool(translate_dict):
            translated = self._transtale_with_key_replace(
                transformed_data, translate_dict)

            self.DB.save_on_db(dict_id, "datatypes.json", translated)

            self._data_types = translated    

    def sort_by_primary_key(self, data: Dict[str, Any], pks: List[str]):
        primary_keys = {key: value for key, value in data.items() if key in pks}
        not_primary_keys = {key: value for key, value in data.items() if key not in pks}

        return {**primary_keys, **not_primary_keys}

    def make_full_dict(self, source_table_name: str, destiny_table_name: str, schema: str, pks: List[str], sort_keys: List[str]):
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

    def load(self, id: str, data: Dict[str, Any], translation_dict: Dict[str, str], raw_datatypes: Dict[str, Any], source_table_name: str, destiny_table_name: str, schema: str, pks: List[str], sort_keys: List[str], path:str, database_raw:str, database_sta:str, translate_obj: Dict[str, str] | None):

        """
        Função para executar a criação do dict completo, são considerados os parâmetros:


        - id: str = Nome da tabela para salvar como pasta 
        (ATENÇÃO! Caso você crie uma tabela sem trocar o valor desse parâmetro, a tabela criada por último substituirá a anterior) 

        - data: Dict[str, Any] = Com base na ordem das chaves desde dicionário, serão ordenados os demais. 
        Trocar a ordem das chaves fará com que as traduções e datatypes também tenham suas ordens trocadas.
        É importante ressaltar que as PRIMARY KEYS e SORT KEYS vão estar no topo sempre, mesmo que você não coloque manualmente neste dict,
        isso foi solicitação para otimização das tabelas.

        - translation_dict: Dict[str, str] = Este deve ser o dicionário que possui as colunas desatualizadas (chaves) e 
        colunas atualizadas (valor), será usado para fazer a tradução do resultado

        - raw_datatypes: str = Este deve ser uma string contendo as colunas e datatypes provindos do mapeamento de origem.

        - source_table_name: str = Este é o nome não traduzido da tabela

        - destiny_table_name: str = Este é o nome traduzido da tabela 

        - schema: str = este é o nome do schema (ovs) da tabela (ex.: "cusreg")

        - pks: List[str] Esta é a lista de Primary Keys da tabela 
        (IMPORTANTE: Colocar as keys sem tradução, pois serão traduzidas automaticamente)

        - sort_keys: List[str] = Esta é a lista de Sort Keys (deduplicação) da tabela
        (IMPORTANTE: Colocar as keys sem tradução, pois serão traduzidas automaticamente)


        - translate_obj: Dict[str, str] | None = Aqui você passa os valores para alterar as os tipos de dados. 
        Por padrão é None;
        """

        self.DB.set_original_data(id, data)
        
        self.make_markdown(id, data, translation_dict, self._handle_data_types.make_dict(raw_datatypes), source_table_name, destiny_table_name, schema, pks, path, database_raw, database_sta)
        
        sort_pks = self.sort_by_primary_key(data, [*pks, *sort_keys])
        self.translate(sort_pks, translation_dict, id)        
        self.handle_data_types(sort_pks, raw_datatypes, translate_obj, id)

        full_dict = self.make_full_dict(source_table_name, destiny_table_name, schema, pks, sort_keys)
        self.DB.save_on_db(id, 'full_dict.json', full_dict)

        print(f"table --> {json.dumps(self.table, indent=4)}")
        print(
            f"translate_dict --> {json.dumps(self.translate_dict, indent=4)}")

        print(f"datatypes --> {json.dumps(self.data_types, indent=4)}")
   
        
    def make_markdown(self, dict_id:str, main_table:dict, tranlated_table:dict, datatypes_dict:dict, source_table_name: str, destiny_table_name: str, schema: str, pks:List[str], path:str, database_raw:str, database_sta:str):
        header = f"""**Source**
        
        Owner/Schema: {schema.upper()}
        Table/File: {source_table_name.upper()}
        Path: {path.lower()}

        **Raw Target**

        Database/Schema: {(database_raw + '.' + schema).lower()}
        Table Name: {source_table_name.lower()}

        **Stg Target**

        Database/Schema: {(database_sta + '.' + schema).lower()}
        Table Name: {destiny_table_name.lower()}

        | cloumn_name | primary key | Data Type | Ordinal Position | Field_Name 
        |--|--|--|--|--|    
        """
        print('datatypes_dict: ', datatypes_dict)
        
        data = [f'''| {key} | {"x" if key in pks else ''} | {({key.lower() : value for key, value in datatypes_dict.items()})[key.lower()]} | {index} | {tranlated_table[key]} |''' for index, [key, value] in enumerate(main_table.items())]
        md = re.sub(r'[ \t]+', '', header) + '\n'.join(data)
        self.DB.save_on_db(dict_id, "markdown.md", md)
        return md
        
