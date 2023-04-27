from typing import Any
from setting import connect_setting_oracle, path
from sqlalchemy import create_engine, text, inspect, Engine
from dataclasses import make_dataclass
import cx_Oracle
import re

from log import _init_logger

logger = _init_logger(name="oracle_table", filename="oracle_table.log")

"""
Подключение к Oracle и функции для выгрузки таблиц
"""

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


def call_oracle_function(function_name: str, args: dict, return_cx_oracle_type) -> Any:
    """
    Вызоп PL/SQL функции
    :param function_name: имя pl/sql функции
    :param args: аргументы функции
    :param return_cx_oracle_type: тип cx_Oracle, который должен вернуться из pl/sql функции
    :return: результат функции
    """

    # подключение к базе oracle
    with cx_Oracle.connect(
            user=connect_setting_oracle['USERNAME'],
            password=connect_setting_oracle['PASSWORD'],
            dsn=connect_setting_oracle['DSN']
    ) as conn:
        cursor = conn.cursor()  # создаем курсор
        args_list = list(args.values())  # создаем список с именами параметров функции
        result = cursor.callfunc(function_name, return_cx_oracle_type, args_list)  # вызываем функцию и записываем результат

    return result


def call_oracle_procedure(procedure_name: str, args: dict = None, out_args: list[str] = None) -> list:
    """
    Вызов pl/sql процедуру
    :param procedure_name:  имя процедуры
    :param args:  словарь с аргументами и их значениями
    :param out_args:  список с именами OUT-аргументов
    :return: значения OUT аргументов
    """

    # подключение к базе oracle
    with cx_Oracle.connect(
            user=connect_setting_oracle['USERNAME'],
            password=connect_setting_oracle['PASSWORD'],
            dsn=connect_setting_oracle['DSN']
    ) as conn:
        cursor = conn.cursor()  # создаем курсор
        args_list = list(args.values())     # создаем список с именами параметров процедуры
        result = cursor.callproc(procedure_name, args_list)     # вызываем процу и записываем результат

    # обнавляем входные значения
    idx = 0
    for key in args:
        args[key] = result[idx]
        idx += 1

    # берем из аргуметнов только выходные (OUT)
    out: list[int] = []
    for key in out_args:
        out.append(args[key])

    return out


def create_sql_table(table_name: str, select: str = None, where: str = None, add_fields: list[tuple] = None) -> dict:
    """
    Создает словарь с суррогатными ключами и в значение уходят все поля таблицы
    Базовый запрос: SELECT {select if select else "table_aliace.*"} FROM {table_name} table_aliace {where if where else ""}

    :param table_name:  имя таблицы
    :param select:  select-зона в запросе
    :param where:  where-зона в запросе
    :param add_fields:  дополнительные поля, если в select-зоне явно указаны ещё другие поля
    :return:  dict[0..n] : dataclass(поля таблицы и их значения)
    """

    out_table = dict()   # словарь, который будет содержать таблицу
    engine = create_engine(ENGINE_PATH, echo=True)
    attr = _get_attr(table_name, engine, add_fields)  # получаем поля таблицы и их типы (int, str)
    meta = _get_meta(table_name, attr)  # получаем dataclass table_name, у которого поля - это атрибуты attr
    with engine.connect() as conn:
        table = conn.execute(text(f'SELECT {select if select else "table_aliace.*"} FROM {table_name} table_aliace {where if where else ""}'))
        idx: int = 0
        for item in table:
            out_table[idx] = meta(*item)
            idx += 1

    return out_table


def get_table(table_name: str, select: str = None, where: str = None, add_fields: list[tuple] = None, fk: bool = False) -> dict:
    """
    по имени таблице получаем словарь, в котором pk - это ключи, а значение - dataclass с остальными
    полями таблицы

    Базовый запрос: SELECT {select if select else "table_aliace.*"} FROM {table_name} table_aliace {where if where else ""}

    :param select:  select-зона в запросе
    :param add_fields:  дополнительные поля, если в select-зоне явно указаны ещё другие поля
    :param table_name: имя таблицы
    :param where: условие для фильтрации таблицы: алиас таблицы TABLE_NAME всегда 'table_aliace'
    :param fk: Включать fk в ключ словаря
    :return: dict[pk] : dataclass(поля таблицы и их значения)
    """
    engine = create_engine(ENGINE_PATH, echo=True)
    attr = _get_attr(table_name, engine, add_fields)  # получаем поля таблицы и их типы (int, str)
    if fk:
        pk_idx = _get_pk_idx(attr, engine, table_name, fk=True)  # получаем индексы pk и fk в строке таблицы
    else:
        pk_idx = _get_pk_idx(attr, engine, table_name)  # получаем индексы элемента-pk в строке таблицы
    attr = _re_attr(pk_idx, attr)  # удаляем все pk из атрибутов, они не должны быть в dataclass
    meta = _get_meta(table_name, attr)  # получаем dataclass table_name, у которого поля - это атрибуты attr

    with engine.connect() as conn:
        table = conn.execute(text(f'SELECT {select if select else "table_aliace.*"} FROM {table_name} table_aliace {where if where else ""}'))
        match pk_idx:
            case pk_lst if len(pk_lst) == 1:
                # если pk не составной, то просто создаем словарь
                out_table = {item[pk_lst[0]]: meta([item[i] for i in range(len(item)) if i != pk_lst[0]]) for item in table}

            case pk_lst if len(pk_lst) > 1:
                # если pk составной, то создаем словарь с составным ключем типа tuple
                out_table = {tuple(item[x] for x in pk_lst): meta([item[i] for i in range(len(item)) if i not in pk_lst]) for item in table}

            case _:
                logger.debug(f"Невалидная таблица {table_name}")
                out_table = {}

    return out_table


def _get_pk(table_name, engine: Engine) -> list:
    """
    Pk таблицы
    :param table_name  имя таблицы
    :param engine  connect
    :return: pk: list  список с pk таблицы
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


def _get_fk(table_name: str, engine: Engine):
    sql = "SELECT ucc.COLUMN_NAME " \
          "FROM USER_CONS_COLUMNS ucc, " \
          "USER_CONSTRAINTS uc " \
          "WHERE uc.CONSTRAINT_NAME = ucc.CONSTRAINT_NAME " \
          f"AND uc.TABLE_NAME = '{table_name}' " \
          "AND uc.CONSTRAINT_TYPE = 'R'"

    fk = list()
    with engine.connect() as conn:
        table = conn.execute(text(sql))
        for item in table:
            fk += list(item)
    fk = [i.lower() for i in fk]

    return fk


def _get_attr(table_name, engine: Engine, add_fields: list[tuple] = None) -> list:
    """
    :param table_name  имя таблицы
    :param engine  connect
    :param: add_fields: дополнительные поля таблицы
    :return attr: список кортежей с полем таблицы и его типом (int, str)
    """
    attr = list()   # список с аргументами
    inspector = inspect(engine)
    columns = inspector.get_columns(f'{table_name}')    # получаем названия полей таблицы

    # сначала закидываем дополнительные поля, т.к. при запросе их результат будет первым
    if add_fields:
        attr += add_fields

    # добавляем все поля таблицы table_name
    for column in columns:
        attr.append((column['name'], _get_type_attr(str(column['type']))))

    return attr


def _get_type_attr(oracle_type):
    """
    Преобразовываем типы данных Oracle к Python
    :param oracle_type: тип данных oracle
    :return: типа данных python
    """
    if re.search('NUMBER', oracle_type) or re.search('INTEGER', oracle_type):
        return int
    if re.search('VARCHAR', oracle_type):
        return str
    return str


def _get_meta(name, attr):
    """
    Создвем метакласс Python с названием name и полями attr
    :param name: имя создаваемого dataclass
    :param attr: список полей класса
    :return: dataclass name с полями attr
    """
    return make_dataclass(str(name), attr)


def _get_pk_idx(attr: list[tuple], engine: Engine, table_name, fk: bool = False) -> list:
    """
    Получаем индексы pk в списке атрибутов. Это нужно для их удаления, при создании метакласса,
    т.к. pk таблицы - это ключи словаря

    :param attr: список полей таблицы
    :param engine:  connect
    :param table_name:  имя таблицы
    :param fk: включаем ли fk в ключ словаря
    :return: pk_idx: список с индексами элементов-pk таблицы
    """
    pk = _get_pk(table_name, engine)  # ищем pk

    if fk:
        fk = _get_fk(table_name, engine)

    pk_idx = list()
    for idx, key in enumerate(attr):
        if key[0] in pk:
            pk_idx.append(idx)
        if fk and key[0] in fk:
            pk_idx.append(idx)

    return pk_idx


def _re_attr(pk_idx: list, attr: list) -> list:
    """
    Удаляем из attr все атрибуты, которые являются pk таблицы
    Они не нужны при создании dataclass

    :param pk_idx: список с индексами pk таблицы
    :param attr: атрибуты таблицы
    :return: список атрибуттов без pk
    """
    for idx in pk_idx:
        attr.pop(idx)

    return attr


if __name__ == "__main__":
    engine = create_engine(ENGINE_PATH, echo=True)
    print(get_table(table_name="TIME_OF_TPD_CHAPTERS", fk=True))

