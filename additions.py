"""
Вспомогательные функции
"""


def range_ty_period(start: int, stop: int) -> list[int]:
    """
    Заполнение списка ty_periods от start до stop
    :param start: ty_period начала дисциплины
    :param stop: ty_period конца дисциплины
    :return: list [start, ... , stop]
    """
    ty_list: list[int] = [start]    # список с учебными перриодами в диапозоне
    cur_ty: int = start
    while cur_ty != stop:
        if str(cur_ty)[-1] == "3":  # если последний символ "3" - увеличиваем год на 1
            year = int(str(cur_ty)[:-1]) + 1
            trim = "1"
            cur_ty = int(str(year) + trim)
        else:   # иначе увеличиваем текущий трим на 1
            year = int(str(cur_ty)[:-1])
            trim = int(str(cur_ty)[-1]) + 1
            cur_ty = int(str(year) + str(trim))

        ty_list.append(cur_ty)

    return ty_list
