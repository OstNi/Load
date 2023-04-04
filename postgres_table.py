from peewee import *
from pwiz import *
from setting import connect_setting_postgres

# Подключение к базе
pg_db = PostgresqlDatabase("postgres",
                           user=connect_setting_postgres['USERNAME'],
                           password=connect_setting_postgres['PASSWORD'],
                           host=connect_setting_postgres['HOST'],
                           port=connect_setting_postgres['PORT']
)

# Models
models = generate_models(pg_db, schema="public", table_names=["stu_groups"])
Stu_groups = models["stu_groups"]


def _get_attr(model: str) -> list:
    """
    По названию модели получаем список её аттребутов
    :param model: название модели
    :return: список аттребутов
    """
    attr: list = []
    # итерируемся по атрибутам класса, исключая Protected Attributes и DoesNotExist
    for item in vars(model):
        if item[0] != '_' and item != 'DoesNotExist':
            attr.append(item)
    return attr


class BaseModel(Model):
    class Meta:
        database = pg_db


class StuGroup(BaseModel):
    sgr_id = AutoField(column_name="sgr_id")
    name = CharField(max_length=240)
    dgr_id = IntegerField()
    sgr_sgr_id = IntegerField()
    info = CharField(max_length=1000)





