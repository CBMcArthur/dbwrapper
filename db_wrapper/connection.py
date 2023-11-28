from dotenv import dotenv_values, find_dotenv

from sqlalchemy import URL, create_engine
from sqlalchemy.exc import OperationalError
from libraries import validation
from libraries import logging_utils

env = dotenv_values(find_dotenv())

class DatabaseConnection:
    def __init__(self, host, port, user, password, database):
        self.db_engine = None

        logging_utils.configure_logging()
        self.logger = logging_utils.get_logger(__name__)
        self.host = host
        self.port = port
        self.user = user
        self.database = database
        self.validate_parameters(self.host, self.port, self.user, password or env['postgres_password'], self.database)

        # Create the URl for the connection string
        self.db_url = URL.create('postgresql',
            username=self.user,
            password=password or env['postgres_password'],
            host=self.host,
            port=self.port,
            database=self.database
        )
        self.db_engine = self.create_engine()


    def validate_parameters(self, host, port, user, password, database):
        if not all ([host, port, user, password, database]):
            error_msg = "Connection Creation Error: one or more parameters (host, post, user, password, and/or database) were not provided."
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        if not validation.is_valid_port(port):
            error_msg = "An invalid port number was provided"
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        if not validation.is_valid_hostname(host):
            error_msg = "An invalid hostname was provided"
            self.logger.error(error_msg)
            raise ValueError(error_msg)


    def create_engine(self):
        try:
            # Create DB engine object, then attempt to connect
            self.logger.info(f"Creating database engine with postgresql+psycopg2://{self.db_url.username}:{'*'*len(self.db_url.password)}@{self.db_url.host}:{self.db_url.port}/{self.db_url.database}")
            db_engine = create_engine(self.db_url)
            with db_engine.connect():
                pass
        except OperationalError as e:
            error_msg = f"Failed to create the database engine: {e}"
            self.logger.error(error_msg)
            raise ConnectionError(error_msg)
        return db_engine


    def get_db_engine(self):
        if self.db_engine is None:
            self.create_engine()
        return self.db_engine
