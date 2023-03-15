# Подготовка данных для выгрузки со стороны DIS_GROUP
from oracle_table import get_table
from time import time


def get_dis_groups() -> dict:
    """
    DIS_GROUPS которые входят в 2022 год, т.е.
    начинаются не позже конца 2022 и кончаются не раньше начала 2022
    :return: dict[dgr_id] = dataclass(поле таблицы: значение)
    """

    where = ',DGR_PERIODS dp ' \
            ',DGR_PERIODS dp1 ' \
            'WHERE  table_aliace.DGP_START_ID = dp.DGP_ID ' \
            'AND table_aliace.DGP_STOP_ID = dp1.DGP_ID ' \
            'AND dp.TYP_TYP_ID <= 20223 ' \
            'AND dp1.TYP_TYP_ID >= 20221'

    return get_table('DIS_GROUPS', where)


def get_dis_stadies() -> dict:
    """
    DIS_STUDIES на 2022 учебный период
    :return: dict[dds_id] = dataclass(поле таблицы: значение)
    """
    where = 'WHERE EXISTS (' \
            'SELECT * ' \
            'FROM DGR_PERIODS dp ' \
            ',TY_PERIODS tp ' \
            'WHERE  dp.TYP_TYP_ID = tp.TYP_ID ' \
            'AND tp.ty_ty_id = 2022 ' \
            'AND dp.DSS_DSS_ID = table_aliace.dss_id)'

    return get_table('DIS_STUDIES', where)


if __name__ == '__main__':

    start = time()
    dis_studies = get_dis_stadies()
    end = time()
    print(f'dis_studies time: {round((end - start), 2)} sec | count: {len(dis_studies)}')

    start = time()
    dis_groups = get_dis_groups()
    end = time()
    print(f'dis_groups time: {round((end - start), 2)} sec | count: {len(dis_groups)}')
