# Подготовка данных для выгрузки со стороны DIS_GROUP
from oracle_table import get_table, create_sql_table, procedure
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


def get_tc_time(tc_id: int) -> dict:
    """
    Все TC_TIME по tc_id
    :param tc_id: id времени учебного раздела
    :return: TIME_OF_TPD_CHAPTERS: dict[tc_id] : dataclass(поле таблицы: значение)
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


def _get_type_of_edu_periods(teach_plan: int) -> int:
    """
    :param teach_plan: id учебного плана
    :return: тип учебного периода
    """
    where = f' WHERE tp_id = {teach_plan}'
    return create_sql_table(table_name='teach_plans', where=where)[0].tedp_tedp_id


def _get_args(pr_id: int, dis_id: int) -> tuple[dict, list[str]]:
    """
    Расчет параметров для процедуры 'dgr_utl.dGS_TERMS'
    :param pr_id: id личного дела студента
    :param dis_id: id дисциплины
    :return: tuple[ dict, list[str] ] - словарь с именами аргументов и их значениями, список с именами OUT-параметров
    """
    tp_id = _get_teach_plan_id(pr_id)

    add_attr = [
        ('start_typ_id', int),
        ('stop_typ_id', int),
    ]

    # запрос для создания таблицы с параметрами для процедуры 'dgr_utl.dGS_TERMS'
    # она возвращает триместры начала и конца дисциплины
    select = ' dgp_start.typ_typ_id, dgp_stop.typ_typ_id, table_aliace.* '
    where = 'join dgr_periods dgp_start ' \
            'on(table_aliace.dgp_start_id=dgp_start.dgp_id) ' \
            'join dis_groups dgr ' \
            'on(table_aliace.dgr_dgr_id=dgr.dgr_id) ' \
            'join dgr_periods dgp_stop ' \
            'on(coalesce(table_aliace.dgp_stop_id, dgr.dgp_stop_id)=dgp_stop.dgp_id) ' \
            'JOIN DIS_STUDIES ds '\
            'on(dgr.DSS_DSS_ID = ds.DSS_ID) '\
            'JOIN DISCIPLINES d '\
            'ON(ds.DIS_DIS_ID = d.DIS_ID) '\
            f'where table_aliace.pr_pr_id={pr_id} and d.dis_id = {dis_id}'

    param_table = create_sql_table(table_name='dgr_students', select=select, where=where, add_fields=add_attr)
    args = {
        'P_START_CRS': param_table[0].start_crs,        # курс студента с которого студент начал изучение дисциплины
        'P_START_TYP':  param_table[0].start_typ_id,    # TY_PERIOD начала дисциплины
        'P_STOP_TYP': param_table[0].stop_typ_id,       # TY_PERIOD конца дисциплины
        'P_TEDP_ID': _get_type_of_edu_periods(tp_id),   # тип периода обучения (триместр/семестр)
        'Q_START_TERM': 0,
        'Q_STOP_TERM': 0
    }

    return args, ['Q_START_TERM', 'Q_STOP_TERM']


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
            term_bound = []
            for pr in pr_lst:
                term_bound.append(procedure(procedure_name='dgr_utl.dGS_TERMS',
                                            args=_get_args(pr, discipline_id)[0],
                                            out_args=_get_args(pr, discipline_id)[1]
                                            ))
                tpdl_lst.append(get_tpdl(pr, discipline_id))

            for i in range(len(pr_lst)):
                print(f'tpdl_id: {tpdl_lst[i]} bounds: {term_bound[i]}')
            break



