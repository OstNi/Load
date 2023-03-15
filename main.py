from oracle_table import get_table
from time import time


def main():
    start = time()
    where = 'where ty_ty_id = 2022'
    nr_group = get_table('NR_GROUPS', where)
    end = time()
    print(f'nr_group time: {round((end - start),2)} sec | count: {len(nr_group)}')

    # start = time()
    # dis_study = get_table('DIS_STUDIES')
    # end = time()
    # print(f'dis_study time: {round((end - start), 2)} sec | count: {len(dis_study)}')
    #
    # start = time()
    # tpd_chapters = get_table('TPD_CHAPTERS')
    # end = time()
    # print(f'tpd_chapters time: {round((end - start), 2)} sec | count: {len(tpd_chapters)}')


if __name__ == '__main__':
    main()
