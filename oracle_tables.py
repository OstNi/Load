from sqlalchemy import create_engine, MetaData, Table, Integer, String, \
    Column, DateTime, ForeignKey, Numeric
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class TPD_CHAPTRES(Base):
    __tablename__ = 'TPD_CHAPTERS'
    TC_ID = Column(Integer, primary_key=True)
    SORT = Column(Integer, nullable=False)
    TC_COMMENT = Column(String(4_000), nullable=True)
    EXAM = Column(String(1), nullable=True)
    NAME = Column(String(240), nullable=False)
    TC_TC_ID = Column(Integer, nullable=True)
    TP_TP_ID = Column(Integer, nullable=False)
    TCTP_TCTP_ID = Column(Integer, nullable=True)
    TPDL_TPDL_ID = Column(Integer, nullable=True)




