from postgres_model import *
from dis_group import *
from log import _init_logger
from oracle_table import *
from additions import get_num_of_course
from exceptions import *

from dataclasses import dataclass
from datetime import datetime
from tqdm import tqdm

"""
Алгоритм выгрузки со стороны DIS_GROUP
"""

# инициализируем лог
logger = _init_logger('dis_loading', 'dis_load.log')


def create_stu_group(dis_group: dataclass, dgr_id: int) -> StuGroups | None:
    """
    Создание модели StuGroup postgres_model.py
    :param dis_group: dataclass, содержащии DIS_GOUP
    :param dgr_id: id DIS_GROUP
    :return: объект класса StuGroup postgres_model.py
    """
    if not model_contains(model=StuGroups, key="dgr_id", values=dgr_id):
        # Проверка наличия верхнего уровня

        hight_lvl = None
        if dis_group.dgr_dgr_id:
            if not (hight_lvl := StuGroups.get_or_none(dgr_id=dis_group.dgr_dgr_id)):
                raise StuGroupExc(f"Для группы dgr_id {dgr_id} не было созданого"
                                  f"верхнего уровня dgr_dgr_id {dis_group.dgr_dgr_id}")

        sgr_id = hight_lvl.sgr_id if hight_lvl else None

        new_group = StuGroups.create(
            name=dis_group.fullname,
            dgr_id=dgr_id,
            sgr_sgr_id=sgr_id,
        )
        logger.debug(f"CREATE: STU_GROUP {new_group.sgr_id}")
        return new_group

    return None


def create_tp_delivery(tpdl_id: int, tp_delivery: dataclass, teach_program: dict) -> TpDeliveries | None:
    """
    Создание модели TpDeliveries postgres_model.py
    :param tpdl_id: id схемы доставки
    :param tp_delivery: поля таблицы
    :param teach_program: поля таблицы teach_program
    :return: объект класса TpDeliveries postgres_model.py
    """

    if not model_contains(model=TpDeliveries, key="tpdl_id", values=tpdl_id):
        # Создаем TEACH_PROGRAM
        teach_program = create_teach_program(tp_id=tp_delivery.tp_tp_id, teach_program=teach_program)

        new_delivery = TpDeliveries.create(
            tpdl_id=tpdl_id,
            name=tp_delivery.name,
            tpr_tpr=teach_program.tpr_id
        )
        logger.debug(f"CREATE: TP_DELIVERY {new_delivery.tpdl_id}")
        return new_delivery

    logger.debug(f"TAKE: TP_DELIVERY {tpdl_id}")
    return TpDeliveries.get(tpdl_id=tpdl_id)


def create_tpr_chapters(tpdl_id: int, tc_chapters: dict) -> list:
    """
    Создание модели TprChapters postgres_model.py
    :param tpdl_id: id схемы доставки
    :param tc_chapters: таблица TPD_CHAPTERS
    :return: объект класса TprChapters postgres_model.py
    """
    tpr_chapters = []

    # Получение всех связанных tc_id для tpdl_id
    related_tc_ids = [tc_id for tc_id, value in tc_chapters.items() if value.tpdl_tpdl_id == tpdl_id]

    # Получение существующих записей TprChapters для связанных tc_id
    existing_tpr_chapters = TprChapters.select().where(TprChapters.tc_id.in_(related_tc_ids))
    existing_tpr_chapters_map = {tpr_chapter.tc_id: tpr_chapter for tpr_chapter in existing_tpr_chapters}

    # Создание или использование существующих записей TprChapters
    for tc_id in related_tc_ids:
        if tc_id not in existing_tpr_chapters_map:
            new_chapter = TprChapters.create(
                tc_id=tc_id,
                name=tc_chapters[tc_id].name,
                ext_ext=tc_chapters[tc_id].exam,
                srt=tc_chapters[tc_id].sort,
                tpr_tpr=tc_chapters[tc_id].tp_tp_id,
                tpdl_tpdl=tpdl_id
            )
            tpr_chapters.append(new_chapter)
            logger.debug(f"CREATE: TPR_CHAPTER {new_chapter.tc_id}")
        else:
            logger.debug(f"TAKE: TPR_CHAPTER {tc_id}")
            tpr_chapters.append(existing_tpr_chapters_map[tc_id])

    return tpr_chapters


def create_tc_time(tc_id: int, ty_id: int, tc_time: dict, tch_id: int) -> list:
    """
    Создание модели TcTimes postgres_model.py
    :param tc_id: id учебного раздела
    :param ty_id: id учебного года
    :param tc_time: таблица TIME_OF_TPD_CHAPTER
    :param tch_id: id TprChapter
    :return: объект класса TcTimes postgres_model.py
    """

    tc_time_lst = []

    # Получение всех связанных totc_id для заданного tc_id
    related_totc_ids = [totc_id for totc_id, value in tc_time.items() if value.tc_tc_id == tc_id]

    # Получение существующих записей TcTimes для связанных totc_id
    existing_tc_times = TcTimes.select().where(TcTimes.totc_id in related_totc_ids)
    existing_tc_times_map = {tc_time.totc_id: tc_time for tc_time in existing_tc_times}

    for totc_id in related_totc_ids:
        if totc_id not in existing_tc_times_map:
            agrs_for_func = {
                "P_TC_ID": tc_id,
                "P_TOW_ID": tc_time[totc_id].tow_tow_id,
                "P_TY_ID": ty_id
            }

            count_of_ctl = call_oracle_function(function_name="tpd_t.GET_CTL_RPNT_CNT",
                                                args=agrs_for_func,
                                                return_cx_oracle_type=cx_Oracle.NUMBER)
            new_tc_time = TcTimes.create(
                totc_id=totc_id,
                ctl_count=count_of_ctl if count_of_ctl else 0,
                tch_tch=tch_id,
                val=tc_time[totc_id].value,
                wt_wot=tc_time[totc_id].tow_tow_id
            )

            tc_time_lst.append(new_tc_time)
            logger.debug(f"CREATE: TC_TIME {new_tc_time.totc_id}")
        else:
            logger.debug(f"TAKE: TC_TIME {totc_id}")
            tc_time_lst.append(existing_tc_times_map[totc_id])

    return tc_time_lst


def create_teach_program(tp_id: int, teach_program: dict) -> TeachPrograms:
    """
    Создание модели TeachPrograms postgres_model.py
    :param tp_id: id учебного плана
    :param teach_program: поля таблицы
    :return: объект класса TeachPrograms postgres_model.py
    """
    if not model_contains(model=TeachPrograms, key="tpr_id", values=tp_id):
        new_teach_program = TeachPrograms.create(
            tpr_id=tp_id,
            confirm_date=teach_program[tp_id].date_of_confirm,
            dis_dis=teach_program[tp_id].dis_dis_id,
            practice_form=teach_program[tp_id].practice_form,
            practice_schedule=teach_program[tp_id].practice_schedule,
            protocol=teach_program[tp_id].commend_protocol,
            status=teach_program[tp_id].status,
            tpt_tpt=teach_program[tp_id].ttp_ttp_id
        )

        logger.debug(f"CREATE: TEACH_PROGRAM {tp_id}")
        return new_teach_program

    logger.debug(f"TAKE: TEACH_PROGRAM {tp_id}")
    return TeachPrograms.get(tpr_id=tp_id)


def create_discipline(dis_id: int, discipline: dataclass) -> Disciplines:
    """
    Создание модели Disciplines postgres_model.py
    :param dis_id: id дисциплины
    :param discipline: все поля и значения по dis_id таблицы в метаклассе
    :return: объект класса Disciplines postgres_model.py
    """

    if not model_contains(model=Disciplines, key="dis_id", values=dis_id):
        new_discipline = Disciplines.create(
            dis_id=dis_id,
            name=discipline.name
        )

        logger.debug(f"CREATE: DISCIPLINE {dis_id}")
        return new_discipline

    logger.debug(f"TAKE: DISCIPLINE {dis_id}")
    return Disciplines.get(dis_id=dis_id)


def create_version() -> Versions:
    """
    Создание модели Version postgres_model.py
    :return: объект класса Version postgres_model.py
    """
    new_version = Versions.create(
        calc_date=datetime.now(),
        info="dis_group loading"
    )

    logger.debug(f"CREATE: VERSION {new_version.ver_id}")
    return new_version


def create_group_work(sgr_id: int, type_of_work_id: int) -> GroupWorks:
    """
    Создание модели WorkTypes postgres_model.py
    :param sgr_id: id stu_group
    :param type_of_work_id: id типа работ
    :return:
    """
    new_group_work = GroupWorks.create(
        sgr_sgr=sgr_id,
        wt_wot=type_of_work_id
    )

    logger.debug(f"CREATE: GROUP_WORK {new_group_work.grw_id}")
    return new_group_work


def create_group_faculty(
        dis_groups: dict,
        dgr_id: int,
        sgr_id: int,
        personal_info: dict,
        dgr_students: dict,
        table_for_num_of_course: dict
) -> list | None:
    """
    Создание моделей GroupFaculties postgres_model.py
    :param dis_groups: все DIS_GROUP
    :param dgr_id: id текущей группы
    :param personal_info: словарь с div_id, edu_lvl и foe_id для каждого студента
    :param sgr_id: id stu_group
    :param dgr_students: нужен для поиска студентов в группе
    :param table_for_num_of_course: dict[pr_id, dgr_id]: start_crs, start_typ_id
    """

    # Если это не нижний уровень, то не создаем  GROUP_FACULTY
    if not is_low_dgr_group(dis_groups, dgr_id):
        return

    group_faculties = []
    pl_list = {value.pr_pr_id for value in dgr_students.values() if value.dgr_dgr_id == dgr_id}
    dif_group_fuculty = dict()
    for pr_item in pl_list:
        data = personal_info[pr_item]

        # группируем и считаем студентов по трем параметрам: div_id, foe_id, edu_lvl
        if (data.div_id, data.foe_id, data.edu_lvl) in dif_group_fuculty:
            dif_group_fuculty[(data.div_id, data.foe_id, data.edu_lvl)]["count"] += 1
        else:
            dif_group_fuculty[(data.div_id, data.foe_id, data.edu_lvl)] = {"count": 1, "pr_id": pr_item}

    for info, value in dif_group_fuculty.items():
        # Создаем GROUP_FACULTY
        data = table_for_num_of_course.get((value["pr_id"], dgr_id), [])[0]

        start_crs = data.start_crs
        start_typ_id = data.start_typ_id
        teach_years = 2022

        new_group_faculty = GroupFaculties.create(
            efo_efo=info[1],
            stu_count=value["count"],
            num_course=get_num_of_course(start_crs, start_typ_id, teach_years),
            sgr_sgr=sgr_id,
            div_div=info[0],
            ele_ele=info[2]
        )

        logger.debug(f"CREATE: GROUP_FACULTY {new_group_faculty.grf_id}")
        group_faculties.append(new_group_faculty)

    return group_faculties


def is_low_dgr_group(dis_groups: dict, dgr_id: int) -> bool:
    """
    Определяет: является ли эта группа нижним урровнем
    :param dis_groups: все DIS_GROUPS
    :param dgr_id: id текущей группы
    :return: True - нижний уровень, иначе - False
    """
    if not any(dis_group.dgr_dgr_id == dgr_id for dis_group in dis_groups.values()):
        return True
    return False


def child_dgr_id(dis_groups: dict, dgr_id: int) -> list:
    child = []

    def find_child(dis_groups: dict, dgr_id: int):
        """
        Находи нижний уровень DIS_GROUPS (студенты есть только на нижних уровнях)
        :param dis_groups: все DIS_GROUPS группы
        :param dgr_id: id DIS_GROUP
        :return: dgr_id группы нижнего уровня
        """
        child_dgr_id = [key for key, value in dis_groups.items() if value.dgr_dgr_id == dgr_id]
        if len(child_dgr_id) == 0:
            nonlocal child
            child.append(dgr_id)
        else:
            for item in child_dgr_id:
                find_child(dis_groups, item)
    find_child(dis_groups, dgr_id)
    return child


def choice_of_branch(**kwargs):
    """
    Выбор ветки выгрузки - электив, факультатив, дисциплина по выборру
    """
    if kwargs["dis_study"].tpdl_tpdl_id:
        return create_tp_delivery(tpdl_id=kwargs["dis_study"].tpdl_tpdl_id,
                                  teach_program=kwargs["teach_program"],
                                  tp_delivery=kwargs["tp_deliveries"][kwargs["dis_study"].tpdl_tpdl_id])
    if kwargs["dis_study"].fcr_fcr_id:
        return facultative_branch(fcr_id=kwargs["dis_study"].fcr_fcr_id,
                                  fcr=kwargs["fcr"],
                                  teach_program=kwargs["teach_program"],
                                  tp_delivery=kwargs["tp_deliveries"])
    return dpv_branch(**kwargs)


def facultative_branch(fcr_id: int, fcr: dict, tp_delivery: dict, teach_program: dict) -> TpDeliveries:
    """
    Ветка факультатива. Узнаем tpdl_id и создаем TpDeliveries с указанным tpdl_id
    :param fcr_id: id FACULTATIVE_REQUESTS
    :param fcr: FACULTATIVE_REQUESTS
    :param tp_delivery: метакласс с полями таблицы и их значениями
    :param teach_program: учебная программа. Нужна для во время создания TpDelivery
    :return: объект класса TpDeliveries postgres_model.py
    """
    return create_tp_delivery(
        tpdl_id=fcr[fcr_id].tpdl_tpdl_id,
        tp_delivery=tp_delivery[fcr[fcr_id].tpdl_tpdl_id],
        teach_program=teach_program
    )


def dpv_branch(**kwargs) -> TpDeliveries | None:
    """
    Ветка дисциплины по выбору. Находим нижний уровень, берем личные дела студента и из их
    учебного плана находим схему доставки
    :return: объект класса TpDeliveries postgres_model.py
    """

    # Находим нижний уровень этого дерева (только на нижнем уровне есть связь со студентами)
    low_dgr_id = child_dgr_id(kwargs["dis_groups"], kwargs["dgr_id"])

    # Создаем множество из учебных планов всех студентов нижнего уровня группы
    tp_set = {kwargs["personal_records"][value.pr_pr_id].tp_id for dgr_group in low_dgr_id for value in kwargs["dgr_students"].values() if value.dgr_dgr_id == dgr_group}

    # Создаем множество со схемами доставки группы
    tpdl_set = {value.tpdl_tpdl_id for tp_id in tp_set for key, value in kwargs["tp_components"].items() if
                value.tpl_id == tp_id and value.dis_id == kwargs["dis_id"]}

    tpdl_tp_dict = {idx: {"tp_id": tp_id, "tpdl_id": tpdl_id} for idx, (tp_id, tpdl_id) in
                    enumerate(zip(tp_set, tpdl_set))}

    if len(tpdl_tp_dict) == 0:
        raise TpDeliveryExc(f"Нет студентов на нижнем уровне dgr_id: {kwargs['dgr_id']}")

    # Сравниваем учебные нагрузки среди студентов группы
    if not checker(
            tp_tpdl=tpdl_tp_dict,
            dis_group=kwargs["dis_groups"][kwargs["dgr_id"]],
            dgr_periods=kwargs["dgr_periods"],
            tp_tpd_crossings=kwargs["tp_tpd_crossings"],
            tc_time=kwargs["time_of_tpd_chapters"]
    ):
        raise TpDeliveryExc(f"Разная учебная нагрузка у студентов группы {kwargs['dgr_id']}")

    # Выбираем первую tpdl
    tpdl_id = tpdl_tp_dict[0]["tpdl_id"]
    return create_tp_delivery(tpdl_id=tpdl_id, tp_delivery=kwargs["tp_deliveries"][tpdl_id],
                              teach_program=kwargs["teach_program"])


def main():
    logger.debug("---START OF LOADING---")

    start_time = datetime.now()
    bar = tqdm(desc=f"[*] Выгрузка таблиц", total=18)

    # выгружаем все сущности
    dis_groups = get_dis_groups()
    bar.update(1)
    dis_studies = get_table(table_name="DIS_STUDIES")
    bar.update(1)
    disciplines = get_table(table_name="DISCIPLINES")
    bar.update(1)
    dgr_students = get_table(table_name="DGR_STUDENTS")
    bar.update(1)
    dgr_periods = get_table(table_name="DGR_PERIODS")
    bar.update(1)
    dgr_works = get_table(table_name="DGR_WORKS")
    bar.update(1)
    tpd_chapters = get_table(table_name="TPD_CHAPTERS")
    bar.update(1)
    teach_programs = get_table(table_name="TEACH_PROGRAMS")
    bar.update(1)
    tp_deliveries = get_table(table_name="TP_DELIVERIES")
    bar.update(1)
    tp_tpd_crossings = get_tp_tpd_crossings()
    bar.update(1)
    ty_periods = get_table(table_name="TY_PERIODS")
    bar.update(1)
    terms = get_table(table_name="TERMS")
    bar.update(1)
    tp_components = get_tp_components()
    bar.update(1)
    time_of_tpd_chapters = get_table(table_name="TIME_OF_TPD_CHAPTERS")
    bar.update(1)
    table_for_num_of_course = get_table_for_num_of_course()  # таблица, в которой храниться start_crs и start_typ_id
    bar.update(1)
    personal_records = get_personal_records()
    bar.update(1)
    personal_info = get_group_faculty_info()  # форма обучения, факультет и edu_lvl студентов
    bar.update(1)
    facultative_requests = get_table(table_name='FACULTATIVE_REQUESTS')
    bar.update(1)

    bar.set_description("Table loading completed")
    bar.close()

    logger.debug(f"Время выгрузки словарей {datetime.now() - start_time}")

    # Создаем VERSION
    version = create_version()

    bar = tqdm(desc=f"[*] Процесс выгрузки", total=len(dis_groups))
    for dgr_id, dis_group in dis_groups.items():
        dis_study = dis_studies[dis_group.dss_dss_id]

        if dgr_id not in [15770]:
            continue

        if dis_study.foe_foe_id != 1:  # если это не очная форма обучения, пропускаем
            continue

        logger.debug(f"{'-' * 5}LOADING OF GROUP {dgr_id}{'-' * 5}")
        with database.atomic() as transaction:
            try:
                start_time = datetime.now()

                # Создаем Disciplines
                create_discipline(dis_id=dis_study.dis_dis_id, discipline=disciplines[dis_study.dis_dis_id])

                if not (tp_delivery := choice_of_branch(
                        dis_study=dis_study,
                        dgr_id=dgr_id,
                        dis_groups=dis_groups,
                        dgr_students=dgr_students,
                        dgr_periods=dgr_periods,
                        dis_id=dis_study.dis_dis_id,
                        fcr=facultative_requests,
                        personal_records=personal_records,
                        tp_components=tp_components,
                        tp_deliveries=tp_deliveries,
                        teach_program=teach_programs,
                        terms=terms,
                        tp_tpd_crossings=tp_tpd_crossings,
                        time_of_tpd_chapters=time_of_tpd_chapters,
                )
                ):
                    raise TpDeliveryExc(f"Невозможно создать TpDeliveries DGR_ID {dgr_id}")

                # Создаем STU_GROUP если группа уже создана, то пропускаем
                if not (stu_group := create_stu_group(dis_group, dgr_id)):
                    raise StuGroups(f"Невозможно создать StuGroup DGR_ID {dgr_id}")

                # Создаем TPD_CHAPTERS
                tpr_chapters = create_tpr_chapters(tpdl_id=tp_delivery.tpdl_id, tc_chapters=tpd_chapters)

                # Создаем DGR_PERIOD на каждый ty_period, в котором обучается группа
                for typ_id, tpr_chapter in zip(get_ty_period_range(dis_group, dgr_periods), tpr_chapters):
                    div_for_dgr = get_div_for_dgr(
                        ty_id=ty_periods[typ_id].ty_ty_id,
                        bch_id=dis_studies[dis_group.dss_dss_id].bch_bch_id,
                        dis_id=dis_studies[dis_group.dss_dss_id].dis_dis_id
                    )

                    # Создаем DGR_PERIODS
                    dgr_period = DgrPeriods.create(
                        sgr_sgr=stu_group.sgr_id,
                        tch_tch=tpr_chapter.tch_id,
                        ver_ver=version.ver_id,
                        div_div=div_for_dgr if div_for_dgr else 2221,  # если кафедрру нет, то весим на ПГНИУ
                        typ_typ=typ_id
                    )

                    # Создаем TC_TIME
                    tc_time = create_tc_time(
                        tc_id=tpr_chapter.tc_id,
                        ty_id=ty_periods[typ_id].ty_ty_id,
                        tc_time=time_of_tpd_chapters,
                        tch_id=tpr_chapter.tch_id
                    )

                # Создаем GROUP_WORKS
                for value in dgr_works.values():
                    if value.dgr_dgr_id == dgr_id:
                        group_work = create_group_work(stu_group.sgr_id, value.tow_tow_id)

                # Создаем GROUP_FACULTY
                group_faculties = create_group_faculty(
                    dis_groups=dis_groups,
                    dgr_id=dgr_id,
                    sgr_id=stu_group.sgr_id,
                    personal_info=personal_info,
                    dgr_students=dgr_students,
                    table_for_num_of_course=table_for_num_of_course
                )

                logger.debug(f"SUCCESS: для STU_GROUP {stu_group.sgr_id} была успешно создана из DIS_GROUP {dgr_id}")
                transaction.commit()

            except (StuGroupExc, TpDeliveryExc) as e:
                logger.error(f'ALERT: {e} для DIS_GROUP с dgr_id {dgr_id}')
                transaction.rollback()

            except Exception as e:
                logger.error(f'ERROR: {e} для DIS_GROUP с dgr_id {dgr_id}')
                transaction.rollback()

            finally:
                bar.update(1)
                logger.debug(f"Время выгрузки группы {dgr_id}: {datetime.now() - start_time}")

    bar.close()


if __name__ == "__main__":
    main()
