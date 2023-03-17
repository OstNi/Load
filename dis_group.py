# Подготовка данных для выгрузки со стороны DIS_GROUP
from oracle_table import get_table, create_sql_table
from time import time
import logging
import logging.handlers

# логирование
def _init_logger(name):
    logger = logging.getLogger(name)
    foramat = '%(asctime)s :: %(name)s:%(lineno)s :: %(levelname)s :: %(message)s'
    logger.setLevel(logging.DEBUG)
    sh = logging.StreamHandler()
    sh.setFormatter(logging.Formatter(foramat))
    sh.setLevel(logging.DEBUG)
    fh = logging.handlers.RotatingFileHandler(filename='logs/test.log', maxBytes=125000, backupCount=1)
    fh.setFormatter(logging.Formatter(foramat))
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

    return get_table('DIS_GROUPS', where)


def get_tpd_from_tpdl(tpdl_id: int) -> dict:
    """
    Все TPD_CHHAPTERS с конкретной tpdl_id
    :param tpdl_id:
    :return: TPD_CHAPTERS: dict[tc_id] = dataclass(поле таблицы: значение)
    """
    where = f' WHERE table_aliace.tpdl_tp_dl_id = {tpdl_id}'
    return get_table('TPD_CHAPTERS', where)


def get_tc_time(tc_id: int) -> dict:
    """
    Все TC_TIME по tc_id
    :param tc_id:
    :return: TIME_OF_TPD_CHAPTERS: dict[tc_id] = dataclass(поле таблицы: значение)
    """
    where = f'WHERE table_aliace.tc_tc_id = {tc_id}'
    return get_table('TIME_OF_TPD_CHAPTERS', where)


def get_teach_prog(tp_id: int) -> dict:
    """
    Все TEACH_PROGRAMS по tp_id
    :param tp_id:
    :return: TEACH_PROGRAMS: dict[tp_id] = dataclass(поле таблицы: значение)
    """
    where = f'WHERE table_aliace.tp_id = {tp_id}'
    return get_table('TEACH_PROGRAMS', where)


def get_discipline(dis_id: int) -> dict:
    """
    Все DISCIPLINES по dis_id
    :param dis_id:
    :return: DISCIPLINES: dict[dis] = dataclass(поле таблицы: значение)
    """
    where = f' WHERE table_aliace.dis_id = {dis_id}'
    return get_table('DISCIPLINES', where)


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

    dgr_students = get_table('DGR_STUDENTS', where)
    pr_list = []
    for i in dgr_students.keys():
        pr_list.append(dgr_students[i].pr_pr_id)

    return pr_list


def get_dis_studies() -> dict:
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
            'AND dp.DSS_DSS_ID = table_aliace.dss_id)'

    return get_table('DIS_STUDIES', where)


def get_tpdl_for_fac(fcr_id: int) -> int:
    """
    TPDL_ID для DIS_STUDY с типом 'факультатив'
    :param fcr_id:
    :return: tpdl_id
    """
    where = f'WHERE table_aliace.FCR_ID = {fcr_id}'
    fac_req = get_table('FACULTATIVE_REQUESTS', where)

    return fac_req[(fcr_id,)].tpdl_tpdl_id


def type_of_study(study: type):
    """
    :param study: - dataclass по ключу dss_id словаря  dis_studies
    :return: тип дисциплины (электив, факультатив, дисциплина по выбору)
    """
    if study.tpdl_tpdl_id:
        return 'электив'
    if study.fcr_fcr_id:
        return 'факультатив'
    else:
        return 'дисциплина по выбору'


def type_of_work(dgr_id: int) -> dict:
    where = ' WHERE EXISTS (' \
            'SELECT * ' \
            'FROM DGR_WORKS dw ' \
            f'WHERE dw.DGR_DGR_ID = {dgr_id} ' \
            'AND dw.TOW_TOW_ID  = tow.TOW_ID )'

    return get_table('TYPE_OF_WORKS', where)


def tpdl_check(table: dict) -> int:
    tpdl = set()
    for i in table:
        tpdl.add(table[i].tpdl_tpdl_id)

    if len(tpdl) == 1:
        return tpdl[0]
    else:
        logger.debug(f'{table} имеет несколько tpdl: {[i for i in tpdl]}')


if __name__ == '__main__':
    _init_logger('load')
    logger = logging.getLogger('load.main')

    # dis_studies = get_dis_studies()
    # dis_groups = get_dis_groups()
    pr_list = get_pr_id(229)
    print(*pr_list)