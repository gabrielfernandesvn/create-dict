import re
from typing import Dict


class Handle_Data_Types():

    def __init__(self) -> None:
        pass

    def transform_data_type(self, data: str, translate_obj: dict | None = None):
        """
        'data' precisa ser VARCHAR(n), CHAR(n), NUMBER(n), NUMBER(n,n), DATE

        'translate_obj' é opcional e direcionado para alterar as saídas caso deseje
        """

        translate_object = {"date": "timestamp"}
        obj = {**translate_object, **(translate_obj or {})}

        has_timestamp = bool(re.search(r'timestamp', data, re.IGNORECASE))
        has_blob = bool(re.search(r'blob', data, re.IGNORECASE))
        has_char = bool(re.search(r'char', data, re.IGNORECASE))
        has_number = bool(re.search(r'number', data, re.IGNORECASE))
        has_comma = bool(re.search(r',', data, re.IGNORECASE))
        has_int = bool(re.search(r'int', data, re.IGNORECASE))
        has_datetime = bool(re.search(r'datetime', data, re.IGNORECASE))
        has_decimal = bool(re.search(r'decimal', data, re.IGNORECASE))

        is_timestamp = has_timestamp or has_datetime
        is_string = has_char or has_blob
        is_integer = (has_number and not has_comma) or has_int
        is_decimal = (has_number and has_comma) or has_decimal

        if is_string:
            return translate_obj.get("string", "string") if translate_obj is not None else "string"
        elif is_integer:
            return translate_obj.int.get("int", "int") if translate_obj is not None else "int"
        elif is_timestamp:
            return "timestamp"
        elif is_decimal:
            return {"type": "decimal", "precision": data.split('(')[1].split(',')[0], "scale": data.split('(')[1].split(',')[1].split(')')[0]}
        else:
            return obj[data.lower()]

    def migrate(self, main_dict: Dict[str, str], datatypes_dict: Dict[str, str], translate_obj: Dict[str, str] | None = None):

        datatypes_dict_lower = {
            key.lower(): value for key, value in datatypes_dict.items()}
        migrated = {key: self.transform_data_type(
            datatypes_dict_lower[key.lower()], translate_obj) for key in main_dict.keys()}

        return migrated

    def make_dict(self, datatypes: str, delete_values: bool | None = None):
        without_space = re.sub(r'[ \t]+', ' ', datatypes.strip())
        linhas = without_space.split("\n")
        # print("linhas", linhas, "\n")
        sep_linhas = []
        for linha in linhas:
            line = linha.strip().replace(', ', ',').split(' ')[0:2]
            sep_linhas.append(line)
        filtered = [l for l in sep_linhas if len(l) <= 2]
        # print('sep_linhas', sep_linhas)
        dictt = {l[0]: "" if delete_values else (l[1] if len(l) == 2 else "") for l in filtered}
        return dictt
