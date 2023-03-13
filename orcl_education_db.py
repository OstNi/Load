from setting import connect_setting_oracle, path
from sqlalchemy import create_engine, text
import cx_Oracle
cx_Oracle.init_oracle_client(lib_dir=path['LIB_DIR'], config_dir=path['CONFIG_DIR'])

ENGINE_PATH: str = (
        connect_setting_oracle['DIALECT'] + '+' +
        connect_setting_oracle['SQL_DRIVER'] + '://' +
        connect_setting_oracle['USERNAME'] + ':' +
        connect_setting_oracle['PASSWORD'] + '@' +
        connect_setting_oracle['HOST'] + ':' +
        connect_setting_oracle['PORT'] + '/?service_name=' +
        connect_setting_oracle['SERVICE']
)


def get_table(sql: str, dataclass) -> dict:
    '''
    Получаем словарь из таблицы oracle, в которой key=id и значение это dataclass
    sql - запрос для получения таблицы
    dataclass - датакласс для стрк
    '''
    engine = create_engine(ENGINE_PATH)
    nr_group = dict()
    with engine.connect() as conn:
        groups = conn.execute(text(sql))
        for group in groups:
            nr_group[group[0]] = dataclass(*[i for i in group[1::1]])

    return nr_group
