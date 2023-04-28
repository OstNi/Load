# Подготовка данных для выгрузки со стороны DIS_GROUP
from oracle_table import get_table, create_sql_table
from additions import range_ty_period
from math import ceil
from oracle_table import call_oracle_function
from dataclasses import dataclass
from log import _init_logger
import cx_Oracle

"""
Функции для выгрузки данных со стороны DIS_GROUP
"""

# инициализируем лог
logger = _init_logger(name="dis_group", filename="dis_group.log")


def get_personal_records() -> get_table:
    """
    Personal records + tp_id каждого студента
    """
    select = " tfs.TPL_TP_ID, table_aliace.* "
    where = " ,TP_FOR_STUDENTS tfs " \
            " WHERE table_aliace.pr_id = tfs.PR_PR_ID " \
            " AND tfs.TFS_ID = (SELECT DEK.CURRENT_TFS(pr_id) " \
            "FROM PERSONAL_RECORDS pr " \
            "WHERE pr.PR_ID = tfs.PR_PR_ID) " \
            "ORDER BY table_aliace.PR_ID "
    add_field = [('tp_id', int)]

    return get_table(table_name="PERSONAL_RECORDS", where=where, select=select, add_fields=add_field)


def get_tp_components() -> get_table:
    """
    TP_COMPONENNTS + dis_id и tpl_id (id учебного плана)
    """

    select = " t.TP_TP_ID, tp.DIS_DIS_ID, table_aliace.*"
    where = ",TERMS t " \
            ",TEACH_PROGRAMS tp " \
            "WHERE table_aliace.TER_TER_ID = t.TER_ID " \
            "AND table_aliace.TP_TP_ID = tp.TP_ID "
    add_field = [("tpl_id", int), ("dis_id", int)]

    return get_table(table_name="TP_COMPONENTS", where=where, select=select, add_fields=add_field)


def get_tp_tpd_crossings():
    """
    TP_TPD_CROSSINGS + tpl_id и tpdl_id
    """
    select = "tc.TPDL_TPDL_ID, t.TP_TP_ID, table_aliace.*"
    where = "	,TPD_CHAPTERS tc" \
            " ,terms t " \
            "WHERE table_aliace.TC_TC_ID = tc.TC_ID " \
            "AND table_aliace.TER_TER_ID = t.TER_ID"
    add_field = [("tpdl_id", int), ("tpl_id", int)]

    return get_table(table_name="TP_TPD_CROSSINGS", where=where, select=select, add_fields=add_field)


def get_time_of_tpd_chapters():
    """
    TIME_OF_TPD_CHAPTERS + term_id
    """
    select = " ttc.ter_ter_id, table_aliace.* "
    where = " ,TP_TPD_CROSSINGS ttc " \
            " WHERE table_aliace.TC_TC_ID = ttc.TC_TC_ID "

    add_field = [("ter_id", int)]
    return get_table(table_name="TIME_OF_TPD_CHAPTERS", select=select, where=where, add_fields=add_field)


def get_group_faculty_info(pr_id: int) -> dict:
    """
    Получаем DIVISION, EDUCATION_FORM and
    :param pr_id: id личной записи студента
    :return: dict{"div_id": value, "foe_id": velue}
    """
    add_field = [("div_id", int), ("foe_id", int), ("edu_lvl", int)]
    select = "ffd.div_div_id, ffs.foe_foe_id, tos.TOS_ID, table_aliace.*"

    where = ",ffs_for_divs ffd " \
            ",formeducs_for_spec ffs " \
            ",specs s " \
            ",type_of_ses tos " \
            "where table_aliace.ffd_ffd_id=ffd_id " \
            "and ffd.ffs_ffs_id=ffs_id " \
            f"AND table_aliace.PR_ID = {pr_id} " \
            "AND s.SP_ID = ffs.SP_SP_ID " \
            "AND tos.tos_id = s.TOS_TOS_ID "

    info = create_sql_table(table_name="personal_records", select=select, where=where, add_fields=add_field)

    return {"div_id": info[0].div_id, "foe_id": info[0].foe_id, "edu_lvl": info[0].edu_lvl}


def get_num_of_course(dgr_id: int) -> int:
    """
    :param dgr_id: id группы
    :return: номер курса обучения
    """
    pr_lst = get_pr_list(dgr_id)  # список студентов группы

    # берем первого студента и узнаем номер текущего триметра
    where = "WHERE EXISTS ( " \
            "SELECT * " \
            "FROM PERSONAL_RECORDS pr " \
            f"WHERE pr.PR_ID = {pr_lst[0]} " \
            "AND DEK.CURRENT_TFS(pr.PR_ID) = table_aliace.TFS_ID)"

    tfs = get_table(table_name="TP_FOR_STUDENTS", where=where)
    tfs_id = list(tfs.keys())[0]

    return ceil(tfs[tfs_id].current_term / 3)  # округляем в большую сторону (текущий трим / 3)


def get_count_of_students(dgr_id: int) -> int:
    """
    :param dgr_id: id группы
    :return: количество студентов в DIS_GROUP
    """
    return len(get_pr_list(dgr_id))


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
            'AND dp.DSS_DSS_ID = table_aliace.dss_dss_id)' \
            'ORDER BY table_aliace.dgr_id'

    return get_table(table_name='DIS_GROUPS', where=where)


def get_tpd_from_tpdl(tpdl_id: int) -> dict:
    """
    Все TPD_CHHAPTERS с конкретной tpdl_id
    :param tpdl_id: id схемы доставки
    :return: TPD_CHAPTERS: dict[tc_id] = dataclass(поле таблицы: значение)
    """
    where = f' WHERE table_aliace.tpdl_tpdl_id = {tpdl_id}' \
            ' ORDER BY table_aliace.tc_id'
    return get_table(table_name='TPD_CHAPTERS', where=where)


def get_tc_time(tc_id: int) -> dict:
    """
    TIME_OF_TPD_CHAPTER по tc_id
    :param tc_id: id tpd_chapter
    :return: TIME_OF_TPD_CHAPTERS: dict[totc_id] = dataclass(поле таблицы: значение)
    """
    where = f"WHERE table_aliace.tc_tc_id = {tc_id} "

    return get_table(table_name="TIME_OF_TPD_CHAPTERS", where=where)


def get_teach_program(tp_id: int) -> dict:
    """
    TEACH_PROGRAM по tc_id
    :param tp_id: id teach_program
    :return: TEACH_PROGRAMS: dict[tp_id] = dataclass(поле таблицы: значение)
    """
    where = f"WHERE table_aliace.tp_id = {tp_id}"
    return get_table(table_name="TEACH_PROGRAMS", where=where)


def get_discipline(dis_id: int) -> dict:
    """
    DISCIPLINES по dis_id
    :param dis_id: id discipline
    :return: DISCIPLINES: dict[dis_id] = dataclass(поле таблицы: значение)
    """
    where = f" WHERE table_aliace.dis_id = {dis_id}"
    return get_table(table_name="DISCIPLINES", where=where)


def get_pr_list(dgr_id: int) -> list:
    """
    По id группы получаем список с id PERSONAL RECORD студентов этой группы
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
    По id (PERSONAL RECORD) узнаем учебный план студента
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


def get_ty_period_range(dis_group: dataclass, dgr_periods: dict) -> list[int]:
    """
    :param dis_group: dis_group
    :param dgr_periods: DGR_PERIOD (по нему узнаем периоды начала и конца)
    :return: список ty_periods от начала дисциплины до конца
    """
    return range_ty_period(
        start=dgr_periods[dis_group.dgp_start_id].typ_typ_id,
        stop=dgr_periods[dis_group.dgp_stop_id].typ_typ_id
    )


def get_tpdl_use_pr_dis(pr_id: int, dis_id: int) -> int | None:
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


def get_terms(tp_tpd_crossings: dict, tp_id: int, tpdl_id: int) -> list:
    """
    :param tp_tpd_crossings: TP_TPD_CROSSINGS - сущность пересечения tpd_chhapter и terms
    (нужна, чтобы вытащить триместры по tpdl)
    :param tp_id: id учебного плана
    :param tpdl_id: id схемы доставки
    :return: ter_id, в которых преподается дисциплина со схемой доставки tpdl_id по учебному плану tp_id
    """

    return sorted(value.ter_ter_id for value in tp_tpd_crossings.values() if value.tpl_id == tp_id and value.tpdl_id == tpdl_id)


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

    return get_table(table_name='DIS_STUDIES', where=where)


def get_tpdl_for_fac(fcr_id: int) -> int:
    """
    TPDL_ID для DIS_STUDY с типом 'факультатив'
    :param fcr_id:
    :return: tpdl_id
    """
    where = f'WHERE table_aliace.FCR_ID = {fcr_id}'
    fac_req = get_table(table_name='FACULTATIVE_REQUESTS', where=where)

    return fac_req[(fcr_id,)].tpdl_tpdl_id


def type_of_work(dgr_id: int) -> list[int]:
    """
    Получаем тип работы для группы
    :param dgr_id: id группы
    :return: list с id типа работы
    """
    where = ' WHERE EXISTS (' \
            'SELECT * ' \
            'FROM DGR_WORKS dw ' \
            f'WHERE dw.DGR_DGR_ID = {dgr_id} ' \
            'AND dw.TOW_TOW_ID  = table_aliace.TOW_ID )'

    oracle_type_of_work = get_table(table_name='TYPE_OF_WORKS', where=where)
    return [key[0] for key in oracle_type_of_work.keys()]


def checker(
        tp_tpdl: dict,
        dis_group: dataclass,
        dgr_periods: dict,
        tp_tpd_crossings: dict,
        tc_time: dict
) -> bool:
    """
    Проверяем наличие различия нагрузки по часам у лекции, практик и лаб у студентов одной группы
    :return: True - нагрузка одинаковая / False - нагрузка разная
    """
    ty_periods: list[int] = get_ty_period_range(dis_group=dis_group, dgr_periods=dgr_periods)  # учебные периоды группы
    terms_dict: dict = {}  # key = tpdl_id, value = [terms]
    ch_value: dict = {}  # key = ty_period value = [нагрузки по схемам доставки на этот учебный период]

    # заполнение словаря terms
    for idx, value in tp_tpdl.items():
        # Заполняем словарь terms
        terms_dict[value["tpdl_id"]] = get_terms(
            tp_tpd_crossings=tp_tpd_crossings,
            tp_id=value["tp_id"],
            tpdl_id=value["tpdl_id"]
        )

    for tpdl_id, terms in terms_dict.items():
        # сравниваем количество учебных периодов у студента в схеме доставки  и количеством учебных периодов группы
        if len(terms) != len(ty_periods):
            return False

        for idx, ter_id in enumerate(terms):

            # список со значением нагрузки
            val_array = [0] * 7

            # находим нагрузки по виду работ по заданным tpdl_id и ter_id
            for value in tc_time.values():
                if value.ter_id == ter_id and value.tpdl_tpdl_id == tpdl_id and value.tow_tow_id in [1, 2, 6]:
                    val_array[value.tow_tow_id] = value.value

            # раскидываем нагрузку по ty_period
            if ty_periods[idx] in ch_value:
                ch_value[ty_periods[idx]].append([val_array[1], val_array[2], val_array[6]])
            else:
                ch_value[ty_periods[idx]] = [[val_array[1], val_array[2], val_array[6]]]

    # идем по всем ty_period
    # сравниваем значения нагрузок по значению
    # во внешнем цикле идем по столбца, а во внутреннем по строкам и сравниваем значение со значением первой строки
    # примерная схема этой реализации (в конкретном ty_period)
    # [лек1, лаб1, практ1]
    # [лек2, лаб2, практ2]
    # [лек3, лаб3, практ3]
    # [лек4, лаб4, практ4]

    for ty_period in ch_value:
        for col in range(len(ch_value[ty_period][0])):
            for row in range(1, len(ch_value[ty_period])):
                if ch_value[ty_period][row][col] != ch_value[ty_period][0][col]:
                    return False
    return True


def get_div_for_dgr(ty_id: int, bch_id: int, dis_id: int) -> int | None:
    """
    Кафедра для DGR_PERIOD
    :param ty_id: учебный год
    :param bch_id: id филиала
    :param dis_id: id дисциплины
    :return: id кафедры, за которой закреплена дисциплина
    """
    # дата на которую смотрим закрепление специальности,дисциплины
    charge_ds = call_oracle_function(function_name="charge_pkg.get_charge_point_ds",
                                     args={"R_TY_ID": ty_id},
                                     return_cx_oracle_type=cx_Oracle.Date)

    # указать соответсвие branch.bch_id записи staff_divisions.sdiv_id
    s_div = call_oracle_function(function_name="ffd_pkg.BCH_TO_SDIV",
                                 args={"P_BCH_ID": bch_id},
                                 return_cx_oracle_type=cx_Oracle.NUMBER)

    logger.debug(f"{charge_ds=}, {s_div=}, {dis_id=}")

    # закрепление по приказу
    return call_oracle_function(function_name="calc_charge.dis_for_div",
                                args={
                                    "DIS_ID": dis_id,
                                    "S_DIV_ID": s_div,
                                    "CHARGE_DS": charge_ds},
                                return_cx_oracle_type=cx_Oracle.NUMBER)
