# описание дата классов таблиц oracle
from dataclasses import dataclass


@dataclass
class NrGroup:
    name: str
    fullname: str
    subname: str
    st_cnt: int
    ffs_ffs_id: int
    ng_ng_id: int
    tog_tog_id: int
    ty_ty_id: int
    ffd_ffd_id: int

@dataclass
class DisGroup:
    name: str
    fullname: str
    subname: str
    dss_dss_id: int
    dgr_dgr_id: int
    dgp_start_id: int
    dgp_stop_id: int


@dataclass
class DisStudy:
    dis_dis_id: int
    tedp_tedp_id: int
    div_div_id: int
    foe_foe_id: int
    num: int
    stu_year: int
    bch_bch_id: int
    fcr_fcr_id: int
    tpdl_tpdl_id: int