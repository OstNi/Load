from peewee import *
from postgres_model import *
from log import _init_logger
import logging

# инициализируем лог
_init_logger('load')
logger = logging.getLogger('postgres_load.main')


def insert_stu_groups(data: dict, dgr_id: tuple):
    """
    Добавление STU_GROUP
    :param data: словрь с данными из таблицы
    :param dgr_id: ключ словаря - id группы
    :return: создаем запись
    """
    # проверяем: есть ли уже запись с таким sgr_id в таблице
    if StuGroups.get_or_none(dgr_id=dgr_id[0]):
        logger.debug(f"Запись с dgr_id {dgr_id[0]}")
        return

    # достаем верхний уровень (если он существует), чтобы восстановить ссылку
    sgr_sgr_id = StuGroups.get_or_none(dgr_id=data[dgr_id].dgr_dgr_id)

    # проверяем: создан ли верхний уроовень для этой группы
    if data[dgr_id].dgr_dgr_id and not sgr_sgr_id:
        logger.debug(f"Для записи с dgr_id {dgr_id[0]} не создан верхний уровень dgr_dgr_id {data[dgr_id].dgr_dgr_id}")
        return

    # создаем новую запись
    StuGroups.create(
        name=data[dgr_id].fullname,
        sgr_sgr_id=sgr_sgr_id if sgr_sgr_id else None,
    )


