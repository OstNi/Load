from peewee import *
from log import _init_logger
import logging

"""
Описание postgres моделей
"""

# init logger
_init_logger('load')
logger = logging.getLogger('postgres_model.main')

# Connect
database = PostgresqlDatabase('postgres', **{'host': 'localhost', 'port': 5432, 'user': 'postgres', 'password': ''})


def model_contains(model, key: str, values: int) -> bool:
    """
    Проверрка наличия записи c field_id по полю field
    :param model: имя модели
    :param key: имя поля
    :param values: значение поля
    :return: True - если запись создана / False - запись не создана
    """
    if model.get_or_none(**{key: values}):
        return True
    return False


class BaseModel(Model):
    class Meta:
        database = database


class Divisions(BaseModel):
    chair = CharField(constraints=[SQL("DEFAULT 'n'::character varying")])
    div_div_id = IntegerField(index=True, null=True)
    div_id = AutoField()
    faculty = CharField(constraints=[SQL("DEFAULT 'n'::character varying")])
    name = CharField(unique=True)
    short = CharField()

    class Meta:
        table_name = 'divisions'


class StuGroups(BaseModel):
    sgr_id = AutoField(column_name="sgr_id")
    dgr_id = IntegerField(null=True)
    sup_id = IntegerField(null=True)
    info = CharField(null=True)
    name = CharField(unique=True)
    sgr_sgr_id = ForeignKeyField(column_name='sgr_sgr_id', field='sgr_id', model='self', null=True)

    class Meta:
        table_name = 'stu_groups'


class ExamType(BaseModel):
    ext_id = AutoField(primary_key=True)
    name = CharField(unique=True)
    short = CharField(unique=True)

    class Meta:
        table_name = 'exam_type'


class TeachProgType(BaseModel):
    tp_type = CharField()
    tpt_id = AutoField()
    type_info = CharField(null=True)

    class Meta:
        table_name = 'teach_prog_type'


class Disciplines(BaseModel):
    dis_id = AutoField()
    name = CharField(unique=True)

    class Meta:
        table_name = 'disciplines'


class TeachPrograms(BaseModel):
    confirm_date = DateField(null=True)
    dis_dis = ForeignKeyField(column_name='dis_dis_id', field='dis_id', model=Disciplines)
    info = CharField(null=True)
    practice_form = CharField(null=True)
    practice_schedule = CharField(null=True)
    protocol = CharField(null=True)
    status = CharField()
    tpr_id = AutoField()
    tpt_tpt = ForeignKeyField(column_name='tpt_tpt_id', field='tpt_id', model=TeachProgType)

    class Meta:
        table_name = 'teach_programs'


class TpDeliveries(BaseModel):
    name = CharField()
    tpdl_id = AutoField()
    tpr_tpr = ForeignKeyField(column_name='tpr_tpr_id', field='tpr_id', model=TeachPrograms, null=True)

    class Meta:
        table_name = 'tp_deliveries'


class TprChapters(BaseModel):
    ext_ext = ForeignKeyField(column_name='ext_ext_id', field='ext_id', model=ExamType, null=True)
    info = CharField(null=True)
    name = CharField()
    srt = IntegerField()
    tc_id = IntegerField(null=True)
    tch_id = AutoField()
    tpdl_tpdl = ForeignKeyField(column_name='tpdl_tpdl_id', field='tpdl_id', model=TpDeliveries)

    class Meta:
        table_name = 'tpr_chapters'
        indexes = (
            (('name', 'tpdl_tpdl'), True),
        )


class TeachYears(BaseModel):
    end_date = DateField()
    name = CharField(unique=True)
    start_date = DateField(unique=True)
    ty_id = AutoField()

    class Meta:
        table_name = 'teach_years'


class TyPeriods(BaseModel):
    end_date = DateField(null=True)
    num = IntegerField()
    period_type = CharField()
    start_date = DateField(null=True)
    ty_ty = ForeignKeyField(column_name='ty_ty_id', field='ty_id', model=TeachYears)
    typ_id = AutoField()

    class Meta:
        table_name = 'ty_periods'
        indexes = (
            (('ty_ty', 'num'), True),
        )


class Versions(BaseModel):
    calc_date = DateField(unique=True)
    info = CharField(null=True)
    ver_id = AutoField()

    class Meta:
        table_name = 'versions'


class DgrPeriods(BaseModel):
    dgp_id = AutoField()
    div_div = ForeignKeyField(column_name='div_div_id', field='div_id', model=Divisions)
    sgr_sgr = ForeignKeyField(column_name='sgr_sgr_id', field='sgr_id', model=StuGroups)
    tch_tch = ForeignKeyField(column_name='tch_tch_id', field='tch_id', model=TprChapters)
    typ_typ = ForeignKeyField(column_name='typ_typ_id', field='typ_id', model=TyPeriods)
    ver_ver = ForeignKeyField(column_name='ver_ver_id', field='ver_id', model=Versions)

    class Meta:
        table_name = 'dgr_periods'
        indexes = (
            (('div_div', 'sgr_sgr', 'tch_tch', 'typ_typ', 'ver_ver'), True),
        )


class WorkTypes(BaseModel):
    aud = CharField(null=True)
    include_in_tpd = CharField(null=True)
    name = CharField(unique=True)
    oneday = CharField(constraints=[SQL("DEFAULT 'n'::character varying")])
    short = CharField(null=False)
    srt = IntegerField(null=True)
    wot_id = AutoField()

    class Meta:
        table_name = 'work_types'


class GroupWorks(BaseModel):
    grw_id = AutoField()
    sgr_sgr = ForeignKeyField(column_name='sgr_sgr_id', field='sgr_id', model=StuGroups)
    wt_wot = ForeignKeyField(column_name='wt_wot_id', field='wot_id', model=WorkTypes)

    class Meta:
        table_name = 'group_works'
        indexes = (
            (('sgr_sgr', 'wt_wot'), True),
        )


class EduForms(BaseModel):
    efo_id = AutoField()
    name = CharField(unique=True)
    short = CharField(unique=True)

    class Meta:
        table_name = 'edu_forms'


class EduLevels(BaseModel):
    ele_id = AutoField()
    name = CharField(unique=True)
    short = CharField(null=True, unique=True)
    srt = IntegerField(null=True)

    class Meta:
        table_name = 'edu_levels'


class GroupFaculties(BaseModel):
    div_div = ForeignKeyField(column_name='div_div_id', field='div_id', model=Divisions)
    efo_efo = ForeignKeyField(column_name='efo_efo_id', field='efo_id', model=EduForms)
    ele_ele = ForeignKeyField(column_name='ele_ele_id', field='ele_id', model=EduLevels)
    grf_id = AutoField()
    num_course = IntegerField()
    sgr_sgr = ForeignKeyField(column_name='sgr_sgr_id', field='sgr_id', model=StuGroups)
    stu_count = IntegerField()

    class Meta:
        table_name = 'group_faculties'
        indexes = (
            (('ele_ele', 'efo_efo', 'sgr_sgr', 'div_div', 'num_course'), True),
        )


class TcTimes(BaseModel):
    ctl_count = IntegerField()
    tch_tch = ForeignKeyField(column_name='tch_tch_id', field='tch_id', model=TprChapters)
    tim_id = AutoField()
    totc_id = IntegerField(null=True)
    val = IntegerField()
    wt_wot = ForeignKeyField(column_name='wt_wot_id', field='wot_id', model=WorkTypes)

    class Meta:
        table_name = 'tc_times'
        indexes = (
            (('wt_wot', 'tch_tch'), True),
        )
