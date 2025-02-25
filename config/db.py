import os
import logging
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class DBConnection:

    def __init__(self, enviroment):
        if enviroment == "dev":
            logger.info('Development enviroment')
            self.db_host = os.getenv("DATABASE_HOST_DEV")
            self.db_user = os.getenv("DATABASE_USERNAME_DEV")
            self.db_password = os.getenv("DATABASE_PASSWORD_DEV")
            self.db_name = os.getenv("DATABASE_NAME_DEV")
            self.db_port = int(os.getenv("DATABASE_PORT_DEV"))
        elif enviroment == 'prod':
            logger.info('Production enviroment')
            self.db_host = os.getenv("DATABASE_HOST")
            self.db_user = os.getenv("DATABASE_USERNAME")
            self.db_password = os.getenv("DATABASE_PASSWORD")
            self.db_name = os.getenv("DATABASE_NAME")
            self.db_port = int(os.getenv("DATABASE_PORT"))
        else:
            logger.error('Invalid enviroment')
            raise Exception('Invalid enviroment')

        
    
    def create_motor(self):
        logger.info('Creating engine')
        self.motor = create_engine(f"mysql+pymysql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}")
        return self.motor

