from sqlalchemy import create_engine
from setting import connect_setting_postgres

engine = create_engine(
    connect_setting_postgres['DIALECT'] + '://' +
    connect_setting_postgres['USERNAME'] + ':' +
    connect_setting_postgres['PASSWORD'] + '@' +
    connect_setting_postgres['HOST'] + ':' +
    connect_setting_postgres['PORT'] + '/' +
    connect_setting_postgres['DATABASE']
)

with engine.connect() as conn:
    print('Success')