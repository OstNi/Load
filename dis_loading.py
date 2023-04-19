from postgres_model import *
from dis_group import *
from dataclasses import dataclass
from datetime import datetime


"""
Алгоритм выгрузки со стороны DIS_GROUP
"""


def create_stu_group(dis_group: dataclass, dgr_id: int) -> StuGroups | None:
    """
    Создание модели StuGroup postgres_model.py
    :param dis_group: dataclass, содержащии DIS_GOUP
    :param dgr_id: id DIS_GROUP
    :return: объект класса StuGroup postgres_model.py
    """
    if not model_contains(model=StuGroups, key="dgr_id", values=dgr_id):
        # Проверка наличия верхнего уровня
        if dis_group.dgr_dgr_id:
            if not (hight_lvl := StuGroups.get_or_none(dgr_id=dis_group.dgr_dgr_id)):
                logger.debug(f"Для группы dgr_id {dgr_id} не было созданого "
                             f"верхнего уровня dgr_dgr_id {dis_group.dgr_dgr_id}")

        new_group = StuGroups(
            name=dis_group.fullname,
            dgr_id=dgr_id,
            sgr_sgr_id=hight_lvl.sgr_id if hight_lvl else None,
        ).save()

        return new_group

    return None


def create_tp_delivery(tpdl_id: int) -> TpDeliveries:
    """
    Создание модели TpDeliveries postgres_model.py
    :param tpdl_id: id схемы доставки
    :return: объект класса TpDeliveries postgres_model.py
    """
    oracle_tp_deliveries = get_tpdl(tpdl_id)
    tpdl_key = list(oracle_tp_deliveries.keys())[0]

    if not model_contains(model=TpDeliveries, key="tpdl_id", values=tpdl_id):
        new_delivery = TpDeliveries.create(
            tpdl_id=tpdl_id,
            name=oracle_tp_deliveries[tpdl_key].name,
            tpr_tpr=oracle_tp_deliveries[tpdl_key].tp_tp_id
        )

    return new_delivery if new_delivery else TpDeliveries.get(tpdl_id=tpdl_id)


def create_tpr_chapters(tpdl_id: int) -> list:
    """
    Создание модели TprChapters postgres_model.py
    :param tpdl_id: id схемы доставки
    :return: объект класса TprChapters postgres_model.py
    """
    oracle_tc_chapter = get_tpd_from_tpdl(tpdl_id)
    tc_id = list(oracle_tc_chapter.keys())[0]

    tpr_chapters = []

    if not model_contains(model=TprChapters, key="tc_id", values=tc_id[0]):
        tpr_chapters.append(TprChapters(
            tc_id=tc_id[0],
            name=oracle_tc_chapter[tc_id].name,
            ext_ext=oracle_tc_chapter[tc_id].exam,
            str=oracle_tc_chapter[tc_id].sort,
            tpr_tpr=oracle_tc_chapter[tc_id].tp_tp_id,
            tpdl_tpdl=tpdl_id
        ).save())

    return tpr_chapters


def create_tc_time(tc_id: int, type_of_work_id: int) -> TcTimes:
    """
    Создание модели TcTimes postgres_model.py
    :param tc_id: id учебного раздела
    :param type_of_work_id: id типа работ
    :return: объект класса TcTimes postgres_model.py
    """
    oracle_tc_time = get_tc_time(tc_id, type_of_work_id)
    totc_id = list(oracle_tc_time.keys())[0]

    if not model_contains(model=TcTimes, key="totc_id", values=totc_id[0]):
        new_tc_time = TcTimes(
            totc_id=totc_id[0],
            ctl_count=oracle_tc_time[totc_id].val_check,
            tch_tch=tc_id[0],
            val=oracle_tc_time[totc_id].value,
            wt_wot=type_of_work_id
        ).save()
    return new_tc_time if new_tc_time else TcTimes.get(totc_id=totc_id[0])


def create_teach_program(tp_id: int) -> TeachPrograms:
    """
    Создание модели TeachPrograms postgres_model.py
    :param tp_id: id учебного плана
    :return: объект класса TeachPrograms postgres_model.py
    """
    oracle_teach_program = get_teach_program(tp_id)
    if not model_contains(model=TeachPrograms, key="tpr_id", values=tp_id):
        new_teach_program = TeachPrograms.create(
            tpd_id=tp_id,
            confirm_date=oracle_teach_program[(tp_id,)].date_of_confirm,
            dis_dis=oracle_teach_program[(tp_id,)].dis_dis_id,
            practice_form=oracle_teach_program[(tp_id,)].practice_form,
            practice_schedule=oracle_teach_program[(tp_id,)].practice_schedule,
            protocol=oracle_teach_program[(tp_id,)].commend_protocol,
            status=oracle_teach_program[(tp_id,)].status
        )

    return new_teach_program if new_teach_program else TeachPrograms.get(tpr_id=tp_id)


def create_discipline(dis_id: int) -> Disciplines:
    """
    Создание модели Disciplines postgres_model.py
    :param dis_id: id дисциплины
    :return: объект класса Disciplines postgres_model.py
    """
    oracle_discipline = get_discipline(dis_id)

    if not model_contains(model=Disciplines, key="dis_id", values=dis_id):
        new_discipline = Disciplines.create(
            dis_id=dis_id,
            name=oracle_discipline[(dis_id,)].name
        )

    return new_discipline if new_discipline else Disciplines.get(dis_id=dis_id)


def create_version() -> Versions:
    """
    Создание модели Version postgres_model.py
    :return: объект класса Version postgres_model.py
    """
    new_version = Versions(
        calc_date=datetime.now,
        info="dis_group loading"
    ).save()

    return new_version


def create_group_work(sgr_id: int, type_of_work_id: int) -> WorkTypes:
    """
    Создание модели WorkTypes postgres_model.py
    :param sgr_id: id stu_group
    :param type_of_work_id: id типа работ
    :return:
    """
    new_group_work = GroupWorks(
        sgr_sgr=sgr_id,
        wt_wot=type_of_work_id
    ).save()

    return new_group_work


def create_group_faculty(dis_groups: dataclass, dgr_id: tuple, sgr_id: int) -> None:
    """
    Создание моделей GroupFaculties postgres_model.py
    :param dis_groups: все DIS_GROUP
    :param dgr_id: id текущей группы
    :param sgr_id: id stu_group
    """
    low_dgr_id = find_low_lvl_group(dis_groups, dgr_id)
    pl_list = get_pr_list(low_dgr_id[0])

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
        GroupFaculties(
            efo_efo=info[1],
            stu_count=value,
            num_course=get_num_of_course(low_dgr_id[0]),
            sgr_sgr=sgr_id,
            div_div=info[0],
            ele_ele=info[2]
        ).save()


def is_low_stu_group(sgr_id: int) -> bool:
    """
    Определяет: является ли эта группа нижним урровнем
    :param sgr_id: id stu_group
    :return: True - нижний уровень, иначе - False
    """
    if not any(stu_group.sgr_sgr_id == sgr_id for stu_group in StuGroups.select()):
        return True
    return False


def find_low_lvl_group(dis_groups: dict, dgr_id: tuple) -> tuple:
    """
    Находи нижний уровень DIS_GROUPS (студенты есть только на нижних уровнях)
    :param dis_group: все DIS_GROUPS группы
    :param dgr_id: id DIS_GROUP
    :return: dgr_id группы нижнего уровня
    """
    if not any(value.dgr_dgr_id == dgr_id[0] for value in dis_groups.values()):
        return dgr_id
    return find_low_lvl_group(dis_groups, (dis_groups[dgr_id].dgr_dgr_id, ))


def choice_of_branch(study_type: str, dis_study: dataclass, dis_groups: dataclass, dgr_id: tuple):
    """
    Выбор ветки выгрузки - электив, факультатив, дисциплина по выборру
    :param study_type: тип дисциплины
    :param dis_study: дисциплина, которую посещает группа
    :param dis_groups: все DIS_GROUP
    :param dgr_id: id текущей группы
    """
    branches = {
        "эл": elective_branch(dis_study),
        "фак": facultative_branch(dis_study),
        "дпв": dpv_branch(dis_study, dis_groups, dgr_id)
    }
    return branches.get(study_type)


def elective_branch(dis_study: dataclass) -> TpDeliveries:
    """
    Ветка электива. Узнаем tpdl_id и создаем TpDeliveries с указанным tpdl_id
    :param dis_study: дисциплина, которую посещает группа
    :return: объект класса TpDeliveries postgres_model.py
    """
    tpdl_id: int = dis_study.tpdl_tpdl_id
    return create_tp_delivery(tpdl_id)


def facultative_branch(dis_study: dataclass) -> TpDeliveries:
    """
    Ветка факультатива. Узнаем tpdl_id и создаем TpDeliveries с указанным tpdl_id
    :param dis_study: дисциплина, которую посещает группа
    :return: объект класса TpDeliveries postgres_model.py
    """
    tpdl_id: int = get_tpdl_for_fac(dis_study.fcr_fcr_id)
    return create_tp_delivery(tpdl_id)


def dpv_branch(dis_study: dataclass, dis_groups: dataclass, dgr_id: tuple) -> TpDeliveries | None:
    """
    Ветка дисциплины по выбору. Находим нижний уровень, берем личные дела студента и из их
    учебного плана находим схему доставки
    :param dis_study: дисциплина, кототрую посещает группа
    :param dis_groups: все DIS_GROUP
    :param dgr_id: id текущей грруппы
    :return: объект класса TpDeliveries postgres_model.py
    """
    # Находим нижний уровень этого дерева (только на нижнем уровне есть связь со студентами)
    low_dgr_id = find_low_lvl_group(dis_groups, dgr_id)

    # Берем личные дела студентов с нижнего уровня
    pr_lst = get_pr_list(low_dgr_id[0])

    pr_tpdl_lst = [[i] for i in pr_lst]
    for i in range(len(pr_tpdl_lst)):
        pr_tpdl_lst[i].append(get_tpdl(pr_lst[i], dis_study.dis_dis_id))

    # Сравниваем учебные нагрузки среди студентов группы
    if not checker(dgr_id[0], pr_tpdl_lst):
        logger.debug(f"Error. Группа dgr_id: {dgr_id} не может быть создана")
        return None

    tpdl_id = pr_tpdl_lst[0][1]  # берем TC по первой tpdl

    return create_tp_delivery(tpdl_id)


def main():

    # реализация логики выгрузки
    dis_groups = get_dis_groups()   # выгружаем дис группы
    for key in dis_groups:
        dis_studies = get_dis_studies(dis_groups[key].dss_dss_id)   # получаем dis_studies, которую они посещают
        dds_id = list(dis_studies.keys())[0]

        if dis_studies[dds_id].foe_foe_id != 1:     # если это не очная форма обучения, пропускаем
            continue

        # Получаем и создаем TpDelivery исходя из типа дисциплины
        # проверка нужна на случай, если учебная нагрузка среди студентов различна,
        # в этом случае, мы просто не создаем группу
        if not(tp_delivery := choice_of_branch(
                study_type=type_of_study(dis_studies[dds_id]),
                dis_study=dis_studies[dds_id],
                dis_groups=dis_groups,
                dgr_id=key)):
            continue

        # Создаем STU_GROUP если группа уже создана, то пропускаем
        if not (stu_group := create_stu_group(dis_groups[key], key[0])):
            continue

        # Узнаем тип работ у группы
        work_type: int = type_of_work(key[0])

        # Создаем TC
        tpr_chapters = create_tpr_chapters(tp_delivery.tpdl_id)

        # Создаем TEACH_PROGRAM
        teach_program = create_teach_program(tp_delivery.tpr_tpr)

        # Создаем DISCIPLINE
        discipline = create_discipline(teach_program.dis_dis)

        # Создаем VERSION
        version = create_version()

        # Создаем DGR_PERIOD на каждый ty_period, в котором обучается группа
        for typ_id, tpr_chapter in zip(get_ty_period_range(key[0]), tpr_chapters):

            # Создаем DGR_PERIODS
            dgr_period = DgrPeriods(
                sgr_sgr=stu_group.sgr_id,
                tch_tch=tpr_chapter.tch_id,
                ver_ver=version.ver_id,
                #div_div=
                typ_typ=typ_id
            ).save()

            # Создаем TC_TIME
            tc_time = create_tc_time(tpr_chapter.tc_id, work_type)

            # Вызываем TyPeriod, чтобы узнать TeachYears (ty_id)
            ty_period = TyPeriods.get(typ_id=typ_id)

        # Создаем GROUP_WORKS
        group_work = create_group_work(stu_group.sgr_id, work_type)

        # Создаем GROUP_FACULTY
        create_group_faculty(dis_groups, key, stu_group.sgr_id)







