from orcl_education_db import get_table
from time import time


def main():
    start = time()
    sql = 'SELECT NG_ID, NAME, FULLNAME, SUBNAME, ST_CNT, FFS_FFS_ID, ' \
          'NG_NG_ID, TOG_TOG_ID, TY_TY_ID, FFD_FFD_ID FROM NR_GROUPS'
    nr_group = get_table('NR_GROUPS')
    end = time()
    print(f'nr_group time: {round((end - start),2)} sec | count: {len(nr_group)}')

    # start = time()
    # sql = 'SELECT DSS_ID, DIS_DIS_ID, TEDP_TEDP_ID, DIV_DIV_ID, FOE_FOE_ID, NUM, ' \
    #       'STU_YEAR, BCH_BCH_ID, FCR_FCR_ID, TPDL_TPDL_ID FROM DIS_STUDIES'
    # dis_study = get_table(sql, DisStudy)
    # end = time()
    # print(f'dis_study time: {round((end - start), 2)} sec | count: {len(dis_study)}')


if __name__ == '__main__':
    main()
