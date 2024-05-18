from index import params
from tables import Tables


table = Tables()

table.load(
    params['id'].upper(), 
    params['main_dict'],
    params['translation_dict'], 
    params['raw_datatypes'], 
    params['source_table_name'], 
    params['destiny_table_name'], 
    params['schema'], 
    params['pks'], 
    params['sort_keys'], 
    params['path'], 
    params['database_raw'], 
    params['database_sta'],
    params['translate_obj']
)
