from setting import connect_setting_oracle, path
from sqlalchemy import create_engine, Table, Column, Integer, MetaData, select, String, text
import cx_Oracle
import numpy as np
import pandas as pd
from dataclasses import dataclass


cx_Oracle.init_oracle_client(lib_dir=path['LIB_DIR'],
                             config_dir=path['CONFIG_DIR'])

ENGINE_PATH: str = (
        connect_setting_oracle['DIALECT'] + '+' +
        connect_setting_oracle['SQL_DRIVER'] + '://' +
        connect_setting_oracle['USERNAME'] + ':' +
        connect_setting_oracle['PASSWORD'] + '@' +
        connect_setting_oracle['HOST'] + ':' +
        connect_setting_oracle['PORT'] + '/?service_name=' +
        connect_setting_oracle['SERVICE']
)

@dataclass
class ses:
    line_2: str
    # l3: int
    # l4: int
    # l5: int
    # l6: str
    # l7: int
    # l8: str


dict_1 = dict()

engine = create_engine(ENGINE_PATH)
with engine.connect() as conn:
    result = conn.execute(text('SELECT TOS_ID, TYPE_INFO  FROM TYPE_OF_SES'))
    for r in result:
        dict_1[r[0]] = ses(r[1])
    # data = result.fetchall()
    # print(result.keys())
    # for i in data:
    #     print(i)

print(dict_1)
# df = pd.read_sql_table('TYPE_OF_SES', con=engine)
# print(df)


