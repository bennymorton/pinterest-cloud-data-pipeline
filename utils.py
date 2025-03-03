import json
import random
import requests
import yaml
from sqlalchemy import create_engine
from sqlalchemy.sql import text

class DBUtils:
    '''
    Class used to connect to an AWS database, then extract data from it.

    Methods
    -------
    read_db_creds: open a hidden external credentials file then return the creds.
    init_db_engine: use the creds from read_db_creds to initialise a sqlaclhemy engine to interact with the database.
    extract_data: connect with the database using the above two methods, then extract one row of data to be returned.
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
        
class PostingUtils(DBUtils):
    '''
    The methods in this class serve to emulate the process of a user posting to Pinterest, generating data in three different datasets.
    '''
    def __init__(self) -> None:
        self.headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
    
    def data_send(self, topic, payload, invoke_url):
        response = requests.request(
            "POST",
            invoke_url.format(topic),
            headers=self.headers,
            data=payload
            )
        response_full = ('response:' + str(response.status_code) + str(response.text))
        return response_full

    def payload_generator(self, table, topic, datatype='batch'):
        result = self.extract_data(table)
        payload = json.dumps({"records": [{"value": result}]}, default=str)

        if datatype == 'streaming':
            payload = json.dumps({
                "StreamName": topic,
                "Data": result,
                        "PartitionKey": "desired-name"
                        }, default=str)

        return payload