from postgres_model import *
from oracle_table import *

"""
Выгрузка справочников
"""


def exam_type_load():
    """
    Выгрузка всех exam_type
    """
    oracle_exam_type = get_table(table_name="TYPE_OF_EXAMS")
    for key, value in oracle_exam_type.items():
        if not model_contains(model=ExamType, key="ext_id", values=key[0]):
            ExamType.create(
                ext_id=key[0],
                name=value.name,
                short=value.short_name
            )


def work_type_load():
    """
    Выгрузка всех work_types
    """
    oracle_work_type = get_table(table_name="TYPE_OF_WORKS")
    for key, value in oracle_work_type.items():
        if not model_contains(model=WorkTypes, key="wot_id", values=key[0]):
            WorkTypes.create(
                wot_id=key[0],
                name=value.name,
                short=value.short_name,
                aud=value.aud,
                include_in_tpd=value.include_in_tpd,
                oneday=value.oneday,
                str=value.sort,
            )


def teach_year_load():
    """
    Выгрузка всех teach_years
    """
    oracle_teach_year = get_table(table_name="TEACH_YEARS")
    for key, value in oracle_teach_year.items():
        if not model_contains(model=TeachYears, key="ty_id", values=key[0]):
            TeachYears.create(
                ty_id=key[0],
                name=value.name,
                start_date=value.start_date,
                end_date=value.end_date
            )


def education_form_load():
    """
    Выгрузка всех edu_forms
    """
    oracle_education_form = get_table(table_name="FORM_OF_EDUCATIONS")
    for key, value in oracle_education_form.items():
        if not model_contains(model=EduForms, key="efo_id", values=key[0]):
            EduForms.create(
                efo_id=key[0],
                name=value.name,
                short=value.short_name
            )


def edu_levels_load():
    """
    Выгрузка всех edu_levels
    """
    oracle_edu_levels = get_table(table_name="TYPE_OF_SES")
    for key, value in oracle_edu_levels.items():
        if not model_contains(model=EduLevels, key="ele_id", values=key[0]):
            EduLevels.create(
                ele_id=key[0],
                name=value.type_info,
                srt=value.sort,
                short=value.prog_type
            )


def division_load():
    """
    Выгрузка всех divisions
    """
    oracle_division = create_sql_table(table_name="DIVISIONS")
    for key, value in oracle_division.items():
        if value.bch_bch_id in (1, 3) and value.kod_id in (2, 3):
            if not model_contains(model=Divisions, key="div_id", values=value.div_id):
                Divisions.create(
                    div_id=value.div_id,
                    div_div_id=value.div_div_id,
                    name=value.div_office_title,
                    short=value.div_short_title,
                    chair="y" if value.kod_id == 3 and value.div_is_spec in ("n", "y") else "n",
                    faculty="y" if value.kod_id in (2, 3) and value.div_is_spec == "y" else "n"
                )


def teach_porg_type_load():
    """
    Выгрузка TEACH_PROG_TYPE
    """
    oracle_teach_prog_type = get_table(table_name="TYPE_OF_TEACH_PROGS")
    for key, value in oracle_teach_prog_type.items():
        if not model_contains(model=TeachProgType, key="tpt_id", values=key[0]):
            TeachProgType.create(
                tpt_id=key[0],
                tp_type=value.tp_type,
                type_info=value.type_info
            )


def main():
    exam_type_load()
    edu_levels_load()
    education_form_load()
    work_type_load()
    teach_year_load()
    division_load()
    teach_porg_type_load()


if __name__ == "__main__":
    main()

