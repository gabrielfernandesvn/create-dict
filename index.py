from tables import Tables
from migrate import Handle_Data_Types




raw_dict = """attrib_01 des_attrib_01
    attrib_02 des_attrib_02
    attrib_03 des_attrib_03
    attrib_04 des_attrib_04
    attrib_05 des_attrib_05
    attrib_06 des_attrib_06
    attrib_07 des_attrib_07
    attrib_08 des_attrib_08
    attrib_09 des_attrib_09
    attrib_10 des_attrib_10
    attrib_11 des_attrib_11
    attrib_12 des_attrib_12
    attrib_13 des_attrib_13
    attrib_14 des_attrib_14
    attrib_15 des_attrib_15
    attrib_16 des_attrib_16
    attrib_17 des_attrib_17
    attrib_18 des_attrib_18
    attrib_19 des_attrib_19
    attrib_20 des_attrib_20
    attrib_21 des_attrib_21
    attrib_22 des_attrib_22
    attrib_23 des_attrib_23
    attrib_24 des_attrib_24
    attrib_25 des_attrib_25
    attrib_26 des_attrib_26
    attrib_27 des_attrib_27
    attrib_28 des_attrib_28
    attrib_29 des_attrib_29
    attrib_30 des_attrib_30
    attrib_31 des_attrib_31
    attrib_32 des_attrib_32
    attrib_33 des_attrib_33
    attrib_34 des_attrib_34
    attrib_35 des_attrib_35
    attrib_36 des_attrib_36
    attrib_37 des_attrib_37
    attrib_38 des_attrib_38
    attrib_39 des_attrib_39
    attrib_40 des_attrib_40
    attrib_41 des_attrib_41
    attrib_42 des_attrib_42
    attrib_43 des_attrib_43
    attrib_44 des_attrib_44
    attrib_45 des_attrib_45
    attrib_46 des_attrib_46
    attrib_47 des_attrib_47
    conflict_id ide_conflict
    created dat_creation
    created_by ide_created_by
    db_last_upd dat_db_last_update
    db_last_upd_src des_db_last_update_source
    last_upd dat_last_update
    last_upd_by ide_last_update_by
    modification_num num_modification
    par_row_id ide_par_row
    row_id ide_row
    x_attrib_01 des_x_attrib_01
    x_attrib_02 des_x_attrib_02
    x_attrib_03 des_x_attrib_03"""

to_translation_dict = """attrib_01 des_attrib_01
    attrib_02 des_attrib_02
    attrib_03 des_attrib_03
    attrib_04 des_attrib_04
    attrib_05 des_attrib_05
    attrib_06 des_attrib_06
    attrib_07 des_attrib_07
    attrib_08 des_attrib_08
    attrib_09 des_attrib_09
    attrib_10 des_attrib_10
    attrib_11 des_attrib_11
    attrib_12 des_attrib_12
    attrib_13 des_attrib_13
    attrib_14 des_attrib_14
    attrib_15 des_attrib_15
    attrib_16 des_attrib_16
    attrib_17 des_attrib_17
    attrib_18 des_attrib_18
    attrib_19 des_attrib_19
    attrib_20 des_attrib_20
    attrib_21 des_attrib_21
    attrib_22 des_attrib_22
    attrib_23 des_attrib_23
    attrib_24 des_attrib_24
    attrib_25 des_attrib_25
    attrib_26 des_attrib_26
    attrib_27 des_attrib_27
    attrib_28 des_attrib_28
    attrib_29 des_attrib_29
    attrib_30 des_attrib_30
    attrib_31 des_attrib_31
    attrib_32 des_attrib_32
    attrib_33 des_attrib_33
    attrib_34 des_attrib_34
    attrib_35 des_attrib_35
    attrib_36 des_attrib_36
    attrib_37 des_attrib_37
    attrib_38 des_attrib_38
    attrib_39 des_attrib_39
    attrib_40 des_attrib_40
    attrib_41 des_attrib_41
    attrib_42 des_attrib_42
    attrib_43 des_attrib_43
    attrib_44 des_attrib_44
    attrib_45 des_attrib_45
    attrib_46 des_attrib_46
    attrib_47 des_attrib_47
    conflict_id ide_conflict
    created dat_creation
    created_by ide_created_by
    db_last_upd dat_db_last_update
    db_last_upd_src des_db_last_update_source
    last_upd dat_last_update
    last_upd_by ide_last_update_by
    modification_num num_modification
    par_row_id ide_par_row
    row_id ide_row
    x_attrib_01 des_x_attrib_01
    x_attrib_02 des_x_attrib_02
    x_attrib_03 des_x_attrib_03"""

raw_datatypes = """ROW_ID VARCHAR2(15 CHAR) Yes
CREATED DATE Yes sysdate 
CREATED_BY VARCHAR2(15 CHAR) Yes
LAST_UPD DATE Yes sysdate 
LAST_UPD_BY VARCHAR2(15 CHAR) Yes
MODIFICATION_NUM NUMBER(10,0) Yes 0
CONFLICT_ID VARCHAR2(15 CHAR) Yes '0' 
PAR_ROW_ID VARCHAR2(15 CHAR) Yes
ATTRIB_08 CHAR(1 CHAR) Yes
ATTRIB_09 CHAR(1 CHAR) Yes
ATTRIB_10 CHAR(1 CHAR) Yes
ATTRIB_11 CHAR(1 CHAR) Yes
ATTRIB_12 timestamp Yes
ATTRIB_13 datetime Yes
ATTRIB_14 NUMBER(22, 7) Yes
ATTRIB_15 NUMBER(22, 7) Yes
ATTRIB_16 NUMBER(22, 7) Yes
ATTRIB_17 NUMBER(22, 7) Yes
ATTRIB_18 NUMBER(22,7) Yes
ATTRIB_19 NUMBER(22,7) Yes
ATTRIB_20 NUMBER(22,7) Yes
ATTRIB_21 NUMBER(22,7) Yes
ATTRIB_22 NUMBER(22,7) Yes
ATTRIB_23 NUMBER(22,7) Yes
ATTRIB_24 NUMBER(22,7) Yes
ATTRIB_25 decimal(22,7) Yes
ATTRIB_26 DATE Yes
ATTRIB_27 DATE Yes
ATTRIB_28 DATE Yes
ATTRIB_29 DATE Yes
ATTRIB_30 DATE Yes
ATTRIB_31 DATE Yes
ATTRIB_32 DATE Yes
ATTRIB_33 DATE Yes
DB_LAST_UPD DATE Yes
ATTRIB_01 VARCHAR2(100 CHAR) Yes
ATTRIB_02 VARCHAR2(100 CHAR) Yes
ATTRIB_03 VARCHAR2(30 CHAR) Yes
ATTRIB_04 VARCHAR2(30 CHAR) Yes
ATTRIB_05 VARCHAR2(30 CHAR) Yes
ATTRIB_06 VARCHAR2(30 CHAR) Yes
ATTRIB_07 VARCHAR2(30 CHAR) Yes
ATTRIB_34 VARCHAR2(50 CHAR) Yes
ATTRIB_35 VARCHAR2(50 CHAR) Yes
ATTRIB_36 VARCHAR2(50 CHAR) Yes
ATTRIB_37 VARCHAR2(50 CHAR) Yes
ATTRIB_38 VARCHAR2(50 CHAR) Yes
ATTRIB_39 VARCHAR2(50 CHAR) Yes
ATTRIB_40 VARCHAR2(50 CHAR) Yes
ATTRIB_41 VARCHAR2(50 CHAR) Yes
ATTRIB_42 VARCHAR2(50 CHAR) Yes
ATTRIB_43 VARCHAR2(50 CHAR) Yes
ATTRIB_44 VARCHAR2(100 CHAR) Yes
ATTRIB_45 VARCHAR2(100 CHAR) Yes
ATTRIB_46 VARCHAR2(1000 CHAR) Yes
ATTRIB_47 VARCHAR2(255 CHAR) Yes
DB_LAST_UPD_SRC VARCHAR2(50 CHAR) Yes
X_ATTRIB_01 VARCHAR2(30 CHAR) Yes
X_ATTRIB_02 VARCHAR2(30 CHAR) Yes
X_ATTRIB_03 VARCHAR2(30 CHAR) Yes
"""


table = Tables()

datatypes = Handle_Data_Types()
translation_dict = datatypes.make_dict(to_translation_dict)
main_dict = datatypes.make_dict(raw_dict, True)

params = {
    'id':'nome_da_tabela',
    'main_dict': main_dict,
    'translation_dict': translation_dict,
    'raw_datatypes': raw_datatypes,
    'source_table_name': 's_src',
    'destiny_table_name': 's_srcv',
    'schema': 'crm',
    'pks': ['attrib_12', 'attrib_05', 'attrib_08'],
    'sort_keys': ['attrib_13'],
    'path': 'raw/oracle/siebel/siebel/s_src/',
    'database_raw': 'gnr_br_dev_pci_raw',
    'database_sta': 'gnr_br_dev_ext_sta',
    'translate_obj': None
}


# digite na linha de comando ./run
