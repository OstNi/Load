# Подготовка данных для выгрузки со стороны DIS_GROUP
from oracle_table import get_table, create_sql_table, procedure
from additions import range_ty_period
from log import _init_logger
import logging

# инициализируем лог
_init_logger('load')
logger = logging.getLogger('load.main')


# логирование

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
    :param tpdl_id: id схемы доставки
    :return: TPD_CHAPTERS: dict[tc_id] = dataclass(поле таблицы: значение)
    """
    where = f' WHERE table_aliace.tpdl_tp_dl_id = {tpdl_id}'
    return get_table('TPD_CHAPTERS', where=where)


def get_pr_id(dgr_id: int) -> list:
    """
    :param dgr_id: id группы
    :return: список pr_id всех студентов группы
    """
    where = f' WHERE dgr_dgr_id = {dgr_id}'
    dgr_students = get_table('DGR_STUDENTS', where=where)
    pr_list = []
    for i in dgr_students.keys():
        pr_list.append(dgr_students[i].pr_pr_id)

    return pr_list


def _get_teach_plan_id(pr_id: int) -> int:
    """
    :param pr_id: id личного дела студента
    :return: id учебного плана студента
    """
    select = " DEK.CURRENT_TFS(pr_id), table_aliace.* "
    where = f" WHERE PR_ID = {pr_id} "
    add_field = [('cur_tfs', int)]

    tfs_dict = create_sql_table('PERSONAL_RECORDS', select=select, where=where, add_fields=add_field)

    tfs_id = tfs_dict[list(tfs_dict.keys())[0]].cur_tfs

    where = f' WHERE tfs_id = {tfs_id}'
    return create_sql_table(table_name='TP_FOR_STUDENTS', where=where)[0].tpl_tp_id


def _get_ty_period(dgr_id: int) -> list[int]:
    """
    :param dgr_id: id dis_group
    :return: список ty_periods от начала дисциплины до конца
    """
    select = 'dp_s.TYP_TYP_ID, dp_k.TYP_TYP_ID, table_aliace.*'
    where = ' ,DGR_PERIODS dp_s ' \
            ',DGR_PERIODS dp_k ' \
            'WHERE dp_s.DGP_ID = table_aliace.DGP_START_ID  ' \
            'AND dp_k.dgp_id = table_aliace.DGP_STOP_ID  ' \
            f'AND table_aliace.DGR_ID = {dgr_id}'

    add_attr = [
        ('start_ty_period', int),
        ('stop_ty_period', int)
    ]

    ty_period = create_sql_table(table_name='DIS_GROUPS', where=where, select=select, add_fields=add_attr)
    start, stop = ty_period[0].start_ty_period, ty_period[0].stop_ty_period
    return range_ty_period(start, stop)


def get_tpdl(pr_id: int, dis_id: int) -> int | None:
    """
    Получаем схему доставки для дисциплины у студента
    :param pr_id: id личной записи студента
    :param dis_id: id дисциплины
    :return: id схемы доставки
    """
    tp_id = _get_teach_plan_id(pr_id)
    where = ',TERMS t ' \
            ',TEACH_PROGRAMS tp ' \
            'WHERE table_aliace.TER_TER_ID = t.TER_ID ' \
            'AND table_aliace.TP_TP_ID = tp.TP_ID ' \
            f'AND t.TP_TP_ID = {tp_id} ' \
            f'AND tp.DIS_DIS_ID = {dis_id}'

    request_table = create_sql_table(table_name='TP_COMPONENTS', where=where)

    if not request_table:
        logger.debug(f' Для pr_id: {pr_id} и dis_id: {dis_id} не существует TP_COMPONENT')
        return None

    return request_table[0].tpdl_tpdl_id if request_table[0].tpdl_tpdl_id else None


def get_trim(pr_id: int, tpdl_id: int) -> list[int]:
    """
    Триместры, в которых преподаётся дисциплина
    :param pr_id: id личного дела студента
    :param tpdl_id: схема доставки
    :return: список TER_ID
    """
    tp_id = _get_teach_plan_id(pr_id)
    select = "t.TER_ID, table_aliace.*"
    where = "	,TPD_CHAPTERS tc " \
            ",TERMS t " \
            "WHERE table_aliace.TER_TER_ID = t.TER_ID " \
            f"AND tc.TPDL_TPDL_ID = {tpdl_id} " \
            "AND table_aliace.TC_TC_ID  = tc.TC_ID " \
            f"AND t.TP_TP_ID = {tp_id}"

    add_attr = [
        ("ter_id", int)
    ]

    table = create_sql_table(table_name="TP_TPD_CROSSINGS", select=select, where=where, add_fields=add_attr)
    return [table[i].ter_id for i in table]


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
            'AND dw.TOW_TOW_ID  = table_aliace.TOW_ID )'

    return get_table('TYPE_OF_WORKS', where=where)


if __name__ == '__main__':

    # реализация логики выгрузки
    dis_groups = get_dis_groups()   # выгружаем дис группы
    for key in dis_groups:
        dis_studies = get_dis_studies(dis_groups[key].dss_dss_id)   # получаем dis_studies, которую они посещают
        dds_id = list(dis_studies.keys())[0]

        if dis_studies[dds_id].foe_foe_id != 1:     # если это не очная форма обучения, пропускаем
            continue

        study_type = type_of_study(dis_studies[dds_id])     # узнаем тип дисциплины
        work_type = type_of_work(key[0])   # узнаем тип работ группы

        if study_type != 'дпв':
            continue

        # ветка электива
        if study_type == 'эл':
            # дергаем чаптер через схему доставки напрямую
            chapter = get_tpd_from_tpdl(dis_studies[dds_id].tpdl_tpdl_id)

        # ветка факультатива
        if study_type == 'фак':
            # через FACULTATIVE_REQUESTS выходим на схему доставки и дергаем чаптер
            chapter = get_tpd_from_tpdl(get_tpdl_for_fac(dis_studies[dds_id].fcr_fcr_id))

        # ветка дисциплины по выбору
        if study_type == 'дпв':
            discipline_id: int = dis_studies[dds_id].dis_dis_id
            pr_lst = get_pr_id(key[0])    # получаем личные дела студентов в группе с id = key
            tpdl_lst = []
            for pr in pr_lst:
                tpdl_lst.append(get_tpdl(pr, discipline_id))

            logger.debug(f"dgr_id: {key[0]}")
            for i in range(len(pr_lst)):
                logger.debug(f'pr_id: {pr_lst[i]} tpdl_id: {tpdl_lst[i]}')
            logger.debug("-"*20)


