from setting import connect_setting_oracle, path
from sqlalchemy import create_engine, text, inspect, Engine
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


def create_sql_table(table_name: str, select: str = None, where: str = None, add_fields: list[tuple] = None) -> dict:
    out_table = dict()
    engine = create_engine(ENGINE_PATH, echo=True)
    attr = _get_attr(table_name, engine, add_fields)  # получаем поля таблицы и их типы (int, str)
    meta = _get_meta(table_name, attr)  # получаем dataclass table_name, у которого поля - это атрибуты attr
    with engine.connect() as conn:
        table = conn.execute(text(f'SELECT {select if select else "table_aliace.*"} FROM {table_name} table_aliace {where if where else ""}'))
        idx: int = 0
        for item in table:
            out_table[idx] = meta(*[i for i in item])
            idx += 1

    return out_table


def get_table(table_name: str, select: str = None, where: str = None, add_fields: list[tuple] = None) -> dict:
    """
    по имени таблице получаем словарь, в котором pk - это ключи, а значение - dataclass с остальными
    полями таблицы

    запрос для редактирования: SELECT table_aliace.* FROM {table_name} table_aliace {where if where else ""}

    :param table_name: имя таблицы
    :param where: условие для фильтрации таблицы: алиас таблицы TABLE_NAME всегда 'table_aliace'
    :return: dict[pk] : dataclass(поля таблицы и их значения)
    """
    out_table = dict()
    engine = create_engine(ENGINE_PATH, echo=True)
    attr = _get_attr(table_name, engine, add_fields)  # получаем поля таблицы и их типы (int, str)
    pk_idx = _get_pk_idx(attr, engine, table_name)  # получаем индексы элемента-pk в строке таблицы
    attr = _re_attr(pk_idx, attr)  # удаляем все pk из атрибутов, они не должны быть в dataclass
    meta = _get_meta(table_name, attr)  # получаем dataclass table_name, у которого поля - это атрибуты attr

    with engine.connect() as conn:
        table = conn.execute(text(f'SELECT {select if select else "table_aliace.*"} FROM {table_name} table_aliace {where if where else ""}'))
        for item in table:
            # т.к. pk может быть несколько в ключи словаря, мы добаляем все pk
            # и закидываем в dataclass все остальные элементы

            out_table[tuple([item[i] for i in pk_idx])] = meta(*[item[i] for i in range(len(item)) if i not in pk_idx])

    return out_table


def _get_pk(table_name, engine: Engine) -> list:
    """
    :param table_name - имя таблицы
    :param engine - connect
    :return: pk: list - список с pk таблицы
    """

    # запрос для нахождения pk таблицы table_name
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


def _get_attr(table_name, engine: Engine, add_fields: list[tuple] = None) -> list:
    """
    :param table_name - имя таблицы
    :param engine - connect
    :param: add_fields - дополнительные поля таблицы
    :return attr: list - список кортежей с полем таблицы и его типом (int, str)
    """
    attr = list()
    inspector = inspect(engine)
    columns = inspector.get_columns(f'{table_name}')
    if add_fields:
        attr += add_fields

    for column in columns:
        attr.append((column['name'], _get_type_attr(str(column['type']))))

    return attr


def _get_type_attr(oracle_type):
    """
    :param oracle_type - тип данных orcale table
    :return тип данных python
    """
    if re.search('NUMBER', oracle_type) or re.search('INTEGER', oracle_type):
        return int
    if re.search('VARCHAR', oracle_type):
        return str
    return str


def _get_meta(name, attr):
    """
    :param name - имя создаваемого dataclass
    :param attr - список полей класса
    :return dataclass name с полями attr
    """
    return make_dataclass(str(name), attr)


def _get_pk_idx(attr: list[tuple], engine: Engine, table_name) -> list:
    """
    :param attr - список полей таблицы
    :param engine - connect
    :param table_name - имя таблицы
    :return pk_idx: list - список с индексами элементов-pk таблицы
    """
    pk = _get_pk(table_name, engine)  # ищем pk
    pk_idx = list()
    for idx, key in enumerate(attr):
        if key[0] in pk:
            pk_idx.append(idx)

    return pk_idx


def _re_attr(pk_idx: list, attr: list) -> list:
    """
    Удаляем из attr все атрибуты, которые являются pk таблицы
    Они не нужны при создании dataclass

    :param pk_idx - список с индексами pk таблицы
    :param attr - атрибуты таблицы
    """
    for idx in pk_idx:
        attr.pop(idx)

    return attr
