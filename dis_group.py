# Подготовка данных для выгрузки со стороны DIS_GROUP
from oracle_table import get_table, create_sql_table
from time import time
import logging
import logging.handlers


# логирование
def _init_logger(name):
    logger = logging.getLogger(name)
    format = '%(asctime)s :: %(name)s:%(lineno)s :: %(levelname)s :: %(message)s'
    logger.setLevel(logging.DEBUG)
    sh = logging.StreamHandler()
    sh.setFormatter(logging.Formatter(format))
    sh.setLevel(logging.DEBUG)
    fh = logging.handlers.RotatingFileHandler(filename='logs/test.log', maxBytes=125000, backupCount=1)
    fh.setFormatter(logging.Formatter(format))
    fh.setLevel(logging.DEBUG)
    logger.addHandler(sh)
    logger.addHandler(fh)
    logger.debug('logger was initialized')


def get_dis_groups() -> dict:
    """
    DIS_GROUPS которые входят в 2022 год, т.е.
    начинаются не позже конца 2022 и кончаются не раньше начала 2022
    :return: DIS_GROUPS: dict[dgr_id] = dataclass(поле таблицы: значение)
    """
    where = 'WHERE EXISTS (' \
            'SELECT * ' \
            'FROM DGR_PERIODS dp ' \
            ',TY_PERIODS tp ' \
            'WHERE  dp.TYP_TYP_ID = tp.TYP_ID ' \
            'AND tp.ty_ty_id = 2022 ' \
            'AND dp.DSS_DSS_ID = table_aliace.dss_dss_id)'

    return get_table('DIS_GROUPS', where=where)


def get_tpd_from_tpdl(tpdl_id: int) -> dict:
    """
    Все TPD_CHHAPTERS с конкретной tpdl_id
    :param tpdl_id:
    :return: TPD_CHAPTERS: dict[tc_id] = dataclass(поле таблицы: значение)
    """
    where = f' WHERE table_aliace.tpdl_tp_dl_id = {tpdl_id}'
    return get_table('TPD_CHAPTERS', where=where)


def get_tc_time(tc_id: int) -> dict:
    """
    Все TC_TIME по tc_id
    :param tc_id:
    :return: TIME_OF_TPD_CHAPTERS: dict[tc_id] = dataclass(поле таблицы: значение)
    """
    where = f'WHERE table_aliace.tc_tc_id = {tc_id}'
    return get_table('TIME_OF_TPD_CHAPTERS', where=where)


def get_teach_prog(tp_id: int) -> dict:
    """
    Все TEACH_PROGRAMS по tp_id
    :param tp_id:
    :return: TEACH_PROGRAMS: dict[tp_id] = dataclass(поле таблицы: значение)
    """
    where = f'WHERE table_aliace.tp_id = {tp_id}'
    return get_table('TEACH_PROGRAMS', where=where)


def get_discipline(dis_id: int) -> dict:
    """
    Все DISCIPLINES по dis_id
    :param dis_id:
    :return: DISCIPLINES: dict[dis] = dataclass(поле таблицы: значение)
    """
    where = f' WHERE table_aliace.dis_id = {dis_id}'
    return get_table('DISCIPLINES', where=where)


def get_pr_id(dgr_id: int) -> list:
    """
    :param dgr_id: id группы
    :return: список pr_id всех студентов группы
    """
    where = 'join dgr_periods dgp_start ' \
            'on(table_aliace.dgp_start_id=dgp_start.dgp_id) ' \
            'join dis_groups dgr ' \
            'on(table_aliace.dgr_dgr_id=dgr.dgr_id) ' \
            'join dgr_periods dgp_stop ' \
            'on(coalesce(table_aliace.dgp_stop_id,dgr.dgp_stop_id)=dgp_stop.dgp_id) ' \
            f'where dgr.DGR_ID = {dgr_id} '

    dgr_students = get_table('DGR_STUDENTS', where=where)
    pr_list = []
    for i in dgr_students.keys():
        pr_list.append(dgr_students[i].pr_pr_id)

    return pr_list


def get_current_term(pr_list: list):
    """
    :param pr_list: список id личных дел студентов
    :return: dict с текущими триместрами студентов
    """
    select = " DEK.CURRENT_TFS(pr_id), table_aliace.* "
    where = f" WHERE PR_ID IN ({' ,'.join(list(map(str, pr_list)))}) "
    add_field = [('cur_tfs', int)]

    tfs_dict = create_sql_table('PERSONAL_RECORDS', select=select, where=where, add_fields=add_field)

    cur_tfs = []
    for key in tfs_dict:
        cur_tfs.append(tfs_dict[key].cur_tfs)

    where = f" WHERE tfs_id in ({' ,'.join(map(str,cur_tfs))})"

    return get_table('TP_FOR_STUDENTS', where=where)


def get_dis_studies(dss_id: int) -> dict:
    """
    DIS_STUDIES на 2022 учебный период
    :return: DIS_STUDIES: dict[dds_id] = dataclass(поле таблицы: значение)
    """
    where = 'WHERE EXISTS (' \
            'SELECT * ' \
            'FROM DGR_PERIODS dp ' \
            ',TY_PERIODS tp ' \
            'WHERE  dp.TYP_TYP_ID = tp.TYP_ID ' \
            'AND tp.ty_ty_id = 2022 ' \
            'AND dp.DSS_DSS_ID = table_aliace.dss_id) ' \
            f'AND table_aliace.dss_id = {dss_id}'

    return get_table('DIS_STUDIES', where=where)


def get_tpdl_for_fac(fcr_id: int) -> int:
    """
    TPDL_ID для DIS_STUDY с типом 'факультатив'
    :param fcr_id:
    :return: tpdl_id
    """
    where = f'WHERE table_aliace.FCR_ID = {fcr_id}'
    fac_req = get_table('FACULTATIVE_REQUESTS', where)

    return fac_req[(fcr_id,)].tpdl_tpdl_id


def type_of_study(study: type) -> str:
    """
    :param study: - dataclass по ключу dss_id словаря  dis_studies
    :return: тип дисциплины (электив, факультатив, дисциплина по выбору)
    """
    if study.tpdl_tpdl_id:
        return 'эл'     # электив
    if study.fcr_fcr_id:
        return 'фак'    # факультатив

    return 'дпв'        # дисциплина по выбору


def type_of_work(dgr_id: int) -> dict:
    where = ' WHERE EXISTS (' \
            'SELECT * ' \
            'FROM DGR_WORKS dw ' \
            f'WHERE dw.DGR_DGR_ID = {dgr_id} ' \
            'AND dw.TOW_TOW_ID  = tow.TOW_ID )'

    return get_table('TYPE_OF_WORKS', where=where)


if __name__ == '__main__':
    _init_logger('load')
    logger = logging.getLogger('load.main')

    # реализация логики выгрузки
    dis_groups = get_dis_groups()   # выгружаем дис группы
    for key in dis_groups:
        dis_studies = get_dis_studies(dis_groups[key].dss_dss_id)   # получаем dis_studies, которую они посещают
        dds_id: int = list(dis_studies.keys())[0]
        study_type = type_of_study(dis_studies[dds_id])     # узнаем тип дисциплины

        # ветка электива
        if study_type == 'эл':
            # дергаем чаптер через схему доставки напрямую
            chapter = get_tpd_from_tpdl(dis_studies[dds_id].tpdl_tpdl_id)

        # вктка факультатива
        if study_type == 'фак':
            # через FACULTATIVE_REQUESTS выходим на схему доставки и дергаем чаптер
            chapter = get_tpd_from_tpdl(get_tpdl_for_fac(dis_studies[dds_id].fcr_fcr_id))

        # ветка дисциплины по выбору
        if study_type == 'дпв':
            pr_list = get_pr_id(key)    # получаем личные дела студентов в группе с id = key
            cur_term = get_current_term(pr_list)    # узнаем текущий триместр студентов

