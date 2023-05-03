# Подготовка данных для выгрузки со стороны DIS_GROUP
from oracle_table import get_table
from log import _init_logger
from additions import range_ty_period
from oracle_table import call_oracle_function

from dataclasses import dataclass
from itertools import groupby
from operator import attrgetter
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


def get_group_faculty_info() -> dict:
    """
    Получаем DIVISION, EDUCATION_FORM and EDU_LVL
    :return: dict[pr_id]: edu_lvl, foe_id, div_id
    """
    add_field = [("div_id", int), ("foe_id", int), ("edu_lvl", int)]
    select = "ffd.div_div_id, ffs.foe_foe_id, tos.TOS_ID, table_aliace.*"

    where = ",ffs_for_divs ffd " \
            ",formeducs_for_spec ffs " \
            ",specs s " \
            ",type_of_ses tos " \
            "where table_aliace.ffd_ffd_id=ffd_id " \
            "and ffd.ffs_ffs_id=ffs_id " \
            "AND s.SP_ID = ffs.SP_SP_ID " \
            "AND tos.tos_id = s.TOS_TOS_ID "

    return get_table(table_name="PERSONAL_RECORDS", select=select, where=where, add_fields=add_field)


def get_table_for_num_of_course():
    select = "dgp_start.typ_typ_id, table_aliace.*"
    where = " join dgr_periods dgp_start " \
            "on(table_aliace.dgp_start_id=dgp_start.dgp_id) " \
            "join dis_groups dgr " \
            "on(table_aliace.dgr_dgr_id=dgr.dgr_id) " \
            "join dgr_periods dgp_stop " \
            "on(coalesce(table_aliace.dgp_stop_id,dgr.dgp_stop_id)=dgp_stop.dgp_id) "
    add_field = [("start_typ_id", int)]

    additions_table = get_table(table_name="DGR_STUDENTS", select=select, where=where, add_fields=add_field)
    group_key = attrgetter("pr_pr_id", "dgr_dgr_id")  # поля класса, по которым будет производиться группироовка

    # сортируем значение словаря полям group_key
    additions_table_values = sorted(additions_table.values(), key=group_key)

    # вместо totc_id делаем ключами словаря group_key
    return {key: list(group) for key, group in groupby(additions_table_values, group_key)}


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


def get_tc_id_for_check(tp_tpd_crossings: dict, tp_id: int, tpdl_id: int) -> list:
    """
    :param tp_tpd_crossings: TP_TPD_CROSSINGS - сущность пересечения tpd_chhapter и terms
    (нужна, чтобы вытащить триместры по tpdl)
    :param tp_id: id учебного плана
    :param tpdl_id: id схемы доставки
    :return: ter_id, в которых преподается дисциплина со схемой доставки tpdl_id по учебному плану tp_id
    """

    return sorted(value.tc_tc_id for value in tp_tpd_crossings.values() if value.tpl_id == tp_id and value.tpdl_id == tpdl_id)


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
    tpd_chapters_dict: dict = {}  # key = tpdl_id, value = [tc_id]
    ch_value: dict = {}  # key = ty_period value = [нагрузки по схемам доставки на этот учебный период]

    # заполнение словаря terms
    for idx, value in tp_tpdl.items():
        # Заполняем словарь terms
        tpd_chapters_dict[value["tpdl_id"]] = get_tc_id_for_check(
            tp_tpd_crossings=tp_tpd_crossings,
            tp_id=value["tp_id"],
            tpdl_id=value["tpdl_id"]
        )

    tow_idxs = [1, 2, 6]    # индексы видов работ: 1 - лекции, 2 - лаб, 6 - практ
    group_key = attrgetter("tc_tc_id", "tpdl_tpdl_id")  # поля класса, по которым будет производиться группироовка
    tc_time_values = sorted(tc_time.values(), key=group_key)    # сортируем значение словаря полям group_key

    # вместо totc_id делаем ключами словаря group_key
    val_groups = {key: list(group) for key, group in groupby(tc_time_values, group_key)}

    for tpdl_id, chapters in tpd_chapters_dict.items():
        if len(chapters) != len(ty_periods):
            return False

        for idx, tc_id in enumerate(chapters):
            val_array = [0] * 7

            # Используем группированные значения
            tc_group = val_groups.get((tc_id, tpdl_id), [])

            for value in tc_group:
                if value.tow_tow_id in tow_idxs:
                    val_array[value.tow_tow_id] = value.value

            ty_period = ty_periods[idx]
            current_val_array = [val_array[1], val_array[2], val_array[6]]

            if ty_period in ch_value:
                ch_value[ty_period].append(current_val_array)
            else:
                ch_value[ty_period] = [current_val_array]

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

    # закрепление по приказу
    return call_oracle_function(function_name="calc_charge.dis_for_div",
                                args={
                                    "DIS_ID": dis_id,
                                    "S_DIV_ID": s_div,
                                    "CHARGE_DS": charge_ds
                                },
                                return_cx_oracle_type=cx_Oracle.NUMBER)
