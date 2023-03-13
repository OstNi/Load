from setting import connect_setting_oracle, path
from sqlalchemy import create_engine, text
import cx_Oracle
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


engine = create_engine(ENGINE_PATH)
# with engine.connect() as conn:
#     result = conn.execute(text('SELECT TOS_ID, TYPE_INFO  FROM TYPE_OF_SES'))
#     for r in result:
#         pass










