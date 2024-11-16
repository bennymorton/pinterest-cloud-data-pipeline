import random
import yaml
from sqlalchemy import create_engine
from sqlalchemy.sql import text

class DBUtils:
    '''
    Class used to connect to an AWS database, then extract data from it.

    Methods
    -------
    read_db_creds

    init_db_engine

    extract_data
    '''
    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as db_creds_file:
            db_creds = yaml.safe_load(db_creds_file)
        return db_creds    
    
    def init_db_engine(self):
        db_creds = self.read_db_creds()

        DATABASE_TYPE = db_creds['DATABASE_TYPE']
        DBAPI = db_creds['DBAPI']
        HOST = db_creds['HOST']
        USER = db_creds['USER']
        PASSWORD = db_creds['PASSWORD']
        DATABASE = db_creds['DATABASE']
        PORT = db_creds['PORT']
        
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return engine

    def extract_data(self, table):
        engine = self.init_db_engine()
        random_row = random.randint(0, 11000)

        with engine.connect() as connection:
            sql_string = text(f"SELECT * FROM {table} LIMIT {random_row}, 1")
            selected_row = connection.execute(sql_string)
            for row in selected_row:
                result = dict(row._mapping) 
            return result