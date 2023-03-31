from unittest import TestCase, main
from dis_group import checker


class CheckerTest(TestCase):
    def test_one_record(self):
        # на вхдоо подается одна tpdl
        dgr_id: int = 13982
        lst = [[326744, 154177]]

        self.assertEqual(checker(dgr_id, lst), True)

    def test_same_tpdl(self):
        # на вход подаются одинаковые tpdl
        dgr_id: int = 13982
        lst = [[326744, 154177], [294727, 154177], [325182, 154177], [325393, 154177], [325078, 154177],
                [325319, 154177], [324986, 154177], [325181, 154177], [325071, 154177], [294552, 154177]]

        self.assertEqual(checker(dgr_id, lst), True)

    def test_dif_num_of_trim(self):
        # количество триместров не совпадает с количеством учебных периодов
        dgr_id: int = 13982
        lst = [[326744, 154177], [369544, 145092]]
        self.assertEqual(checker(dgr_id, lst), False)

    def test_same_period_dif_load(self):
        # количество триместров совпадает, но разная нагрузка
        dgr_id: int = 13700
        lst = [[344120, 145793], [348607, 153978], [344110, 145794]]

        self.assertEqual(checker(dgr_id, lst), False)


if __name__ == "__main__":
    main()