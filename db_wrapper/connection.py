import os

from sqlalchemy import URL, create_engine
from sqlalchemy.exc import OperationalError
from libraries import validation
from libraries import logging_utils

class DatabaseConnection:
    def __init__(self):
        logging_utils.configure_logging()
        self.logger = logging_utils.get_logger(__name__)
        self.db_engine = None


    def validate_parameters(self):
        host = os.getenv('DB_HOST')
        port = os.getenv('DB_PORT')
        database = os.getenv('DB_DATABASE')
        user = os.getenv('DB_USER')

        if not all ([host, port, user, os.getenv('DB_PASSWORD'), database]):
            error_msg = "Connection Creation Error: one or more environment variables (host, post, user, password, and/or database) were not set."
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        if not validation.is_valid_port(port):
            error_msg = "An invalid port number was set."
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        if not validation.is_valid_hostname(host):
            error_msg = "An invalid hostname was set."
            self.logger.error(error_msg)
            raise ValueError(error_msg)

    def create_db_engine(self):
        if self.db_engine is not None:
            return self.db_engine

        self.validate_parameters()
        db_url = create_db_url()

        try:
            # Create DB engine object, then attempt to connect
            self.logger.info(f"Creating database engine with postgresql+psycopg2://{db_url.username}:{'*'*len(db_url.password)}@{db_url.host}:{db_url.port}/{db_url.database}")
            self.db_engine = create_engine(db_url)
            with self.db_engine.connect():
                pass
        except OperationalError as e:
            self.db_engine = None
            error_msg = f"Failed to create the database engine: {e}"
            self.logger.error(error_msg)
            raise ConnectionError(error_msg)
        return self.db_engine


    def get_db_engine(self):
        if self.db_engine is None:
            self.create_db_engine()
        return self.db_engine


def create_db_url():
    return URL.create('postgresql',
                      username=os.getenv('DB_USER'),
                      password=os.getenv('DB_PASSWORD'),
                      host=os.getenv('DB_HOST'),
                      port=os.getenv('DB_PORT'),
                      database=os.getenv('DB_DATABASE')
                      )
