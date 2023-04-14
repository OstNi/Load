from postgres_model import *
from oracle_table import *

"""
Выгрузка справочников
"""


def exam_type_load():
    oracle_exam_type = get_table(table_name="TYPE_OF_EXAMS")
    for key, value in oracle_exam_type.items():
        if not model_contains(model=ExamType, key="ext_id", values=key[0]):
            ExamType.create(
                ext_id=key[0],
                name=value.name,
                short=value.short_name
            )


def work_type_load():
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
    oracle_education_form = get_table(table_name="FORM_OF_EDUCATIONS")
    for key, value in oracle_education_form.items():
        if not model_contains(model=EduForms, key="efo_id", values=key[0]):
            EduForms.create(
                efo_id=key[0],
                name=value.name,
                short=value.short_name
            )


def edu_levels_load():
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
    oracle_division = get_table(table_name="DIVISIONS")
    for key, value in oracle_division.items():
        if not model_contains(model=Divisions, key="div_id", values=key[0]):
            Divisions.create(
                div_id=key[0],
                div_div_id=value.div_div_id,
                name=value.div_office_title,
                short=value.div_short_title,
                #chair
                #faculty
            )


def main():
    exam_type_load()
    edu_levels_load()
    education_form_load()
    work_type_load()
    teach_year_load()


if __name__ == "__main__":
    main()

