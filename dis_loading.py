import cx_Oracle
from postgres_model import *
from dis_group import *
from dataclasses import dataclass
from datetime import datetime
from log import _init_logger
from oracle_table import *

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
                logger.debug(f"Для группы dgr_id {dgr_id} не было созданого "
                             f"верхнего уровня dgr_dgr_id {dis_group.dgr_dgr_id}")

        if hight_lvl:
            sgr_id = hight_lvl.sgr_id
        else:
            sgr_id = None

        new_group = StuGroups.create(
            name=dis_group.fullname,
            dgr_id=dgr_id,
            sgr_sgr_id=sgr_id,
        )
        logger.debug(f"CREATE: STU_GROUP {new_group.sgr_id}")
        return new_group

    logger.debug("FAIL: STU_GROUP was not created")
    return None


def create_tp_delivery(tpdl_id: int) -> TpDeliveries | None:
    """
    Создание модели TpDeliveries postgres_model.py
    :param tpdl_id: id схемы доставки
    :return: объект класса TpDeliveries postgres_model.py
    """
    oracle_tp_deliveries = get_tpdl(tpdl_id)
    tpdl_key = list(oracle_tp_deliveries.keys())[0]

    if not model_contains(model=TpDeliveries, key="tpdl_id", values=tpdl_id):
        # Создаем TEACH_PROGRAM
        teach_program = create_teach_program(oracle_tp_deliveries[tpdl_key].tp_tp_id)

        new_delivery = TpDeliveries.create(
            tpdl_id=tpdl_id,
            name=oracle_tp_deliveries[tpdl_key].name,
            tpr_tpr=teach_program.tpr_id
        )
        logger.debug(f"CREATE: TP_DELIVERY {new_delivery.tpdl_id}")
        return new_delivery

    logger.debug(f"TAKE: TP_DELIVERY {tpdl_id}")
    return TpDeliveries.get(tpdl_id=tpdl_id)


def create_tpr_chapters(tpdl_id: int) -> list:
    """
    Создание модели TprChapters postgres_model.py
    :param tpdl_id: id схемы доставки
    :return: объект класса TprChapters postgres_model.py
    """
    oracle_tc_chapter = get_tpd_from_tpdl(tpdl_id)  # список всех TC с такой tpd_id
    tpr_chapters = []

    for tc_id, value in oracle_tc_chapter.items():
        if not model_contains(model=TprChapters, key="tc_id", values=tc_id[0]):
            new_chapter = TprChapters.create(
                tc_id=tc_id[0],
                name=value.name,
                ext_ext=value.exam,
                srt=value.sort,
                tpr_tpr=value.tp_tp_id,
                tpdl_tpdl=tpdl_id
            )
            tpr_chapters.append(new_chapter)
            logger.debug(f"CREATE: TPR_CHAPTER {new_chapter.tc_id}")
        else:
            logger.debug(f"TAKE: TPR_CHAPTER {tc_id[0]}")
            tpr_chapters.append(TprChapters.get(tc_id=tc_id[0]))

    return tpr_chapters


def create_tc_time(tc_id: int, ty_id: int) -> list:
    """
    Создание модели TcTimes postgres_model.py
    :param tc_id: id учебного раздела
    :param ty_id: id учебного года
    :return: объект класса TcTimes postgres_model.py
    """
    oracle_tc_time = get_tc_time(tc_id)
    tc_time_lst = []

    for totc_id, value in oracle_tc_time.items():

        # аргументы для oracle функции, которая сичтает количество точек контрроля
        agrs_for_func = {
            "P_TC_ID": tc_id,
            "P_TOW_ID": value.tow_tow_id,
            "P_TY_ID": ty_id
        }

        count_of_ctl = call_oracle_function("tpd_t.GET_CTL_RPNT_CNT",
                                            args=agrs_for_func,
                                            return_cx_oracle_type=cx_Oracle.NUMBER)

        if not model_contains(model=TcTimes, key="totc_id", values=totc_id[0]):
            new_tc_time = TcTimes.create(
                totc_id=totc_id[0],
                ctl_count=count_of_ctl if count_of_ctl else 0,
                tch_tch=TprChapters.get(tc_id=tc_id).tch_id,
                val=value.value,
                wt_wot=value.tow_tow_id
            )

            tc_time_lst.append(new_tc_time)
            logger.debug(f"CREATE: TC_TIME {new_tc_time.totc_id}")
        else:
            logger.debug(f"TAKE: TC_TIME {totc_id[0]}")
            tc_time_lst.append(TcTimes.get(totc_id=totc_id[0]))

    return tc_time_lst


def create_teach_program(tp_id: int) -> TeachPrograms:
    """
    Создание модели TeachPrograms postgres_model.py
    :param tp_id: id учебного плана
    :return: объект класса TeachPrograms postgres_model.py
    """
    oracle_teach_program = get_teach_program(tp_id)
    if not model_contains(model=TeachPrograms, key="tpr_id", values=tp_id):
        new_teach_program = TeachPrograms.create(
            tpr_id=tp_id,
            confirm_date=oracle_teach_program[(tp_id,)].date_of_confirm,
            dis_dis=oracle_teach_program[(tp_id,)].dis_dis_id,
            practice_form=oracle_teach_program[(tp_id,)].practice_form,
            practice_schedule=oracle_teach_program[(tp_id,)].practice_schedule,
            protocol=oracle_teach_program[(tp_id,)].commend_protocol,
            status=oracle_teach_program[(tp_id,)].status,
            tpt_tpt=oracle_teach_program[(tp_id,)].ttp_ttp_id
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


def create_group_faculty(dis_groups: dataclass, dgr_id: tuple, sgr_id: int) -> None:
    """
    Создание моделей GroupFaculties postgres_model.py
    :param dis_groups: все DIS_GROUP
    :param dgr_id: id текущей группы
    :param sgr_id: id stu_group
    """

    # Если это не нижний уровень, то не создаем  GROUP_FACULTY
    if not is_low_dgr_group(dis_groups, dgr_id[0]):
        return

    pl_list = get_pr_list(dgr_id[0])
    dif_group_fuculty = dict()
    for pr_item in pl_list:
        data = get_group_faculty_info(pr_item)
        div_id, foe_id, edu_lvl = data["div_id"], data["foe_id"], data["edu_lvl"]

        # группируем и считаем студентов по трем параметрам: div_id, foe_id, edu_lvl
        if (div_id, foe_id, edu_lvl) in dif_group_fuculty:
            dif_group_fuculty[(div_id, foe_id, edu_lvl)] += 1
        else:
            dif_group_fuculty[(div_id, foe_id, edu_lvl)] = 1

    for info, value in dif_group_fuculty.items():
        # Создаем GROUP_FACULTY
        new_group_faculty = GroupFaculties.create(
            efo_efo=info[1],
            stu_count=value,
            num_course=get_num_of_course(dgr_id[0]),
            sgr_sgr=sgr_id,
            div_div=info[0],
            ele_ele=info[2]
        )

        logger.debug(f"CREATE: GROUP_FACULTY {new_group_faculty.grf_id}")


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


def find_low_lvl_group(dis_groups: dict, dgr_id: int) -> int:
    """
    Находи нижний уровень DIS_GROUPS (студенты есть только на нижних уровнях)
    :param dis_groups: все DIS_GROUPS группы
    :param dgr_id: id DIS_GROUP
    :return: dgr_id группы нижнего уровня
    """
    if not any(value.dgr_dgr_id == dgr_id for value in dis_groups.values()):
        return dgr_id
    return find_low_lvl_group(dis_groups, dis_groups[dgr_id].dgr_dgr_id)


def get_type_of_discipline(discipline: dataclass) -> str:
    """
    Тип дисциплины
    :param discipline: поля таблицы
    :return: вид дисциплины эл - электив | фк - факультатив | дпв - дисциплина по выбору
    """
    if discipline.tpdl_tpdl_id:
        return 'эл'
    if discipline.fcr_fcr_id:
        return "фк"

    return "дпв"


def choice_of_branch(study_type: str, dgr_id: int, **kwargs):
    """
    Выбор ветки выгрузки - электив, факультатив, дисциплина по выборру
    :param study_type: тип дисциплины
    :param dis_study: дисциплина, которую посещает группа
    :param dis_groups: все DIS_GROUP
    :param dgr_id: id текущей группы
    """
    branches = {
        "эл": lambda: elective_branch(**kwargs),
        "фк": lambda: facultative_branch(**kwargs),
        "дпв": lambda: dpv_branch(dgr_id, **kwargs)
    }

    return branches.get(study_type)()


def elective_branch(**kwargs) -> TpDeliveries:
    """
    Ветка электива. Узнаем tpdl_id и создаем TpDeliveries с указанным tpdl_id
    :return: объект класса TpDeliveries postgres_model.py
    """
    return create_tp_delivery(kwargs["dis_study"].tpdl_tpdl_id)


def facultative_branch(**kwargs) -> TpDeliveries:
    """
    Ветка факультатива. Узнаем tpdl_id и создаем TpDeliveries с указанным tpdl_id
    :return: объект класса TpDeliveries postgres_model.py
    """
    fcr_id = kwargs["discipline"].fcr_fcr_id
    return create_tp_delivery(kwargs["fcr"][fcr_id].tpdl_tpdl_id)


def dpv_branch(dgr_id: int, **kwargs) -> TpDeliveries | None:
    # dis_study: dataclass, dis_groups: dataclass, dgr_id: tuple
    """
    Ветка дисциплины по выбору. Находим нижний уровень, берем личные дела студента и из их
    учебного плана находим схему доставки
    :return: объект класса TpDeliveries postgres_model.py
    """
    # Находим нижний уровень этого дерева (только на нижнем уровне есть связь со студентами)
    low_dgr_id = find_low_lvl_group(kwargs["dis_group"], dgr_id)

    dgs_id = kwargs["dgr_student"][kwargs["dis_group"][low_dgr_id]]

    # Берем личные дела студентов с нижнего уровня
    pr_lst =

    pr_tpdl_lst = [[i] for i in pr_lst]
    for i in range(len(pr_tpdl_lst)):
        pr_tpdl_lst[i].append(get_tpdl_use_pr_dis(pr_lst[i], dis_study.dis_dis_id))

    # Сравниваем учебные нагрузки среди студентов группы
    if not checker(dgr_id[0], pr_tpdl_lst):
        logger.debug(f"Error. Группа dgr_id: {dgr_id} не может быть создана")
        return None

    tpdl_id = pr_tpdl_lst[0][1]  # берем TC по первой tpdl

    return create_tp_delivery(tpdl_id)


def main():
    logger.debug("---START OF LOADING---")

    # Создаем VERSION
    version = create_version()

    # выгружаем все сущности
    dis_groups = get_dis_groups()
    dis_studies = get_table(table_name="DIS_STUDIES")
    disciplines = get_table(table_name="DISCIPLINES")
    tpd_chapters = get_table(table_name="TPD_CHAPTERS")
    time_of_tpd_chapters = get_table(table_name="TIME_OF_TPD_CHAPTERS")
    personal_records = get_table(table_name="PERSONAL_RECORDS")
    dgr_students = get_table(table_name="DGR_STUDENTS")
    tp_deliveries = get_table(table_name="TP_DELIVERIES")
    facultative_requests = get_table(table_name='FACULTATIVE_REQUESTS')

    for dgr_id, value in dis_groups.items():
        dis_study = dis_studies[dis_groups[dgr_id].dss_dss_id]

        if dis_study.foe_foe_id != 1:  # если это не очная форма обучения, пропускаем
            continue

        logger.debug(f"{'-' * 5}LOADING OF GROUP {dgr_id}{'-' * 5}")
        with database.atomic() as transaction:
            try:
                # Создаем Disciplines
                create_discipline(dis_id=dis_study.dis_dis_id, discipline=disciplines[dis_study.dis_dis_id])



                # Создаем STU_GROUP если группа уже создана, то пропускаем
                if not (stu_group := create_stu_group(dis_groups[key], key[0])):
                    raise Exception("StuGroup уже создана")

                # Создаем TPD_CHAPTERS
                tpr_chapters = create_tpr_chapters(tp_delivery.tpdl_id)

                # Создаем DGR_PERIOD на каждый ty_period, в котором обучается группа
                for typ_id, tpr_chapter in zip(get_ty_period_range(key[0]), tpr_chapters):
                    # Вызываем TyPeriod, чтобы узнать TeachYears (ty_id)
                    ty_period = TyPeriods.get(typ_id=typ_id)

                    div_for_dgr = get_div_for_dgr(
                        ty_id=ty_period.ty_ty.ty_id,
                        bch_id=dis_studies[dds_id].bch_bch_id,
                        dis_id=dis_studies[dds_id].dis_dis_id
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
                    tc_time = create_tc_time(tpr_chapter.tc_id, ty_period.ty_ty.ty_id)

                # Создаем GROUP_WORKS
                for tow_id in type_of_work(key[0]):
                    group_work = create_group_work(stu_group.sgr_id, tow_id)

                # Создаем GROUP_FACULTY
                create_group_faculty(dis_groups, key, stu_group.sgr_id)

                logger.debug(f"SUCCESS: для STU_GROUP {stu_group.sgr_id} была успешно создана из DIS_GROUP {key[0]}")
                transaction.commit()

            except Exception as e:
                logger.debug(f'ERROR: {e} для DIS_GROUP с dgr_id {key[0]}')
                transaction.rollback()


if __name__ == "__main__":
    main()

