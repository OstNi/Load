from postgres_model import *
from dis_group import *
from datetime import datetime


"""
Алгоритм выгрузки со сторорны DIS_GROUP
"""


if __name__ == '__main__':

    # реализация логики выгрузки
    dis_groups = get_dis_groups()   # выгружаем дис группы
    for key in dis_groups:
        dis_studies = get_dis_studies(dis_groups[key].dss_dss_id)   # получаем dis_studies, которую они посещают
        dds_id = list(dis_studies.keys())[0]

        if dis_studies[dds_id].foe_foe_id != 1:     # если это не очная форма обучения, пропускаем
            continue

        # создаем STU_GROUP
        if not model_contains(model=StuGroups, key="dgr_id", values=key[0]):
            # Проверка наличия верхнего уровня
            if dis_groups[key].dgr_dgr_id:
                if not (hight_lvl := StuGroups.get_or_none(dgr_id=dis_groups[key].dgr_dgr_id)):
                    logger.debug(f"Для группы dgr_id {key[0]} не было созданого "
                                 f"верхнего уровня dgr_dgr_id {dis_groups[key].dgr_dgr_id}")

            stu_group = StuGroups(
                name=dis_groups[key].fullname,
                dgr_id=key[0],
                sgr_sgr_id=hight_lvl.sgr_id if hight_lvl else None,
            ).save()

        study_type = type_of_study(dis_studies[dds_id])     # узнаем тип дисциплины
        work_type = type_of_work(key[0])   # узнаем тип работ группы

        # ветка электива
        if study_type == 'эл':
            # дергаем чаптер через схему доставки напрямую
            oracle_tc_chapter = get_tpd_from_tpdl(dis_studies[dds_id].tpdl_tpdl_id)

        # ветка факультатива
        if study_type == 'фак':
            # через FACULTATIVE_REQUESTS выходим на схему доставки и дергаем чаптер
            oracle_tc_chapter = get_tpd_from_tpdl(get_tpdl_for_fac(dis_studies[dds_id].fcr_fcr_id))

        # ветка дисциплины по выбору
        if study_type == 'дпв':
            discipline_id: int = dis_studies[dds_id].dis_dis_id
            pr_lst = get_pr_id(key[0])    # получаем личные дела студентов в группе с id = key
            pr_tpdl_lst = [[i] for i in pr_lst]
            for i in range(len(pr_tpdl_lst)):
                pr_tpdl_lst[i].append(get_tpdl(pr_lst[i], discipline_id))

            if not checker(key[0], pr_tpdl_lst):
                logger.debug(f"Error. Группа dgr_id: {key[0]} не может быть создана")
                continue
            oracle_tc_chapter = get_tpd_from_tpdl(pr_tpdl_lst[0][1])  # берем TC по первой tpdl

        # Создаем TC
        tc_id = list(oracle_tc_chapter.keys())[0]

        if not model_contains(model=TprChapters, key="tch_id", values=tc_id[0]):
            tpd_chapter = TprChapters(
                tch_id=tc_id[0],
                name=oracle_tc_chapter[tc_id].name,
                ext_ext=oracle_tc_chapter[tc_id].exam,
                str=oracle_tc_chapter[tc_id].sort,
                tpr_tpr=oracle_tc_chapter[tc_id].tp_tp_id
            ).save()

        # Создаем TC_TIME
        oracle_tc_time = get_tc_time(tc_id[0])
        totc_id = list(oracle_tc_time.keys())[0]

        if not model_contains(model=TcTimes, key="tim_id", values=totc_id[0]):
            tc_time = TcTimes(
                tim_id=totc_id[0],
                ctl_count=oracle_tc_time[totc_id].val_check,
                tch_tch=tc_id[0],
                val=oracle_tc_time[totc_id].value,
                #wt_wot="TYPE_OF_WORK"
            ).save()

        # Создаем TEACH_PROGRAM
        oracle_teach_program = get_teach_program(oracle_tc_chapter[tc_id].tp_tp_id)
        tp_id = list(oracle_teach_program.keys())[0]

        if not model_contains(model=TeachPrograms, key="tpr_id", values=tp_id[0]):
            teach_program = TeachPrograms(
                tpd_id=tp_id[0],
                confirm_date=oracle_teach_program[tp_id].date_of_confirm,
                dis_dis=oracle_teach_program[tp_id].dis_dis_id,
                practice_form=oracle_teach_program[tp_id].practice_form,
                practice_schedule=oracle_teach_program[tp_id].practice_schedule,
                protocol=oracle_teach_program[tp_id].commend_protocol,
                status=oracle_teach_program[tp_id].status
            ).save()

        # Создаем DISCIPLIN
        oracle_discipline = get_discipline(oracle_teach_program[tp_id].dis_dis_id)
        dis_id = list(oracle_discipline.keys())[0]

        if not model_contains(model=Disciplines, key="dis_id", values=dis_id[0]):
            discipline = Disciplines(
                dis_id=dis_id[0],
                name=oracle_discipline[dis_id].name
            ).save()

        # Создаем VERSION
        version = Versions(
            calc_date=datetime.now,
            info="dis_group loading"
        ).save()

        # Создаем DGR_PERIOD
        dgr_period = DgrPeriods(
            sgr_sgr=stu_group.sgr_id,
            tch_tch=tpd_chapter.tch_id,
            ver_ver=version.ver_id,
            #div_div=
            #typ_typ=
        )







