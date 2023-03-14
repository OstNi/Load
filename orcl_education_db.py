from setting import connect_setting_oracle, path
from sqlalchemy import create_engine, text, inspect, Engine, MetaData, Table
from dataclasses import make_dataclass
import cx_Oracle
import re

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


def get_table(table_name) -> dict:
    out_table = dict()
    engine = create_engine(ENGINE_PATH, echo=True)
    attr = _get_attr(table_name, engine)
    pk_idx = _get_pk_idx(attr, engine, table_name)
    attr = re_attr(pk_idx, attr)
    meta = get_meta(table_name, attr)

    with engine.connect() as conn:
        table = conn.execute(text(f'SELECT * FROM {table_name}'))
        for item in table:
            out_table[tuple([item[i] for i in pk_idx])] = meta(*[item[i] for i in range(len(item)) if i not in pk_idx])

    return out_table


def _get_pk(table_name, engine: Engine) -> list:
    sql = f"SELECT ucc.COLUMN_NAME " \
          "FROM USER_CONSTRAINTS uc " \
          ",USER_CONS_COLUMNS ucc " \
          f"WHERE uc.TABLE_NAME = '{table_name}' " \
          "AND uc.CONSTRAINT_TYPE  = 'P' " \
          "AND ucc.CONSTRAINT_NAME = uc.CONSTRAINT_NAME "

    pk = list()
    with engine.connect() as conn:
        table = conn.execute(text(sql))
        for item in table:
            pk += list(item)
    pk = [i.lower() for i in pk]

    return pk


def _get_attr(table_name, engine: Engine) -> list:
    attr = list()
    inspector = inspect(engine)
    columns = inspector.get_columns(f'{table_name}')
    for column in columns:
        attr.append((column['name'], _get_type_attr(str(column['type']))))

    return attr


def _get_type_attr(oracle_type):
    if re.search('NUMBER', oracle_type) or re.search('INTEGER', oracle_type):
        return int
    if re.search('VARCHAR', oracle_type):
        return str
    return str


def get_meta(name, attr):
    return make_dataclass(str(name), attr)


def _get_pk_idx(attr: list[tuple], engine: Engine, table_name) -> list:
    pk = _get_pk(table_name, engine)
    pk_idx = list()
    for idx, key in enumerate(attr):
        if key[0] in pk:
            pk_idx.append(idx)

    return pk_idx


def re_attr(pk_idx: list, attr: list) -> list:
    for idx in pk_idx:
        attr.pop(idx)

    return attr
