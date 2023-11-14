import time

from sqlalchemy import URL, create_engine, text
from sqlalchemy.exc import OperationalError
from libraries import validation
from libraries import logging_utils

class DatabaseConnection:
    def __init__(self, host, port, user, password, database):
        # Validate the parameters for the DB Connection
        logging_utils.configure_logging()
        self.logger = logging_utils.get_logger(__name__)
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

        # Create the URl for the connection string
        self.db_url = URL.create('postgresql',
            username=user,
            password=password,
            host=host,
            port=port,
            database=database
        )
        try:
            # Create DB engine object, then attempt to connect
            self.logger.info(f"Creating database engine with postgresql+psycopg2://{self.db_url.username}:{'*'*len(self.db_url.password)}@{self.db_url.host}:{self.db_url.port}/{self.db_url.database}")
            self.db_engine = create_engine(self.db_url)
            with self.db_engine.connect():
                pass
        except OperationalError as e:
            error_msg = f"Failed to create the database engine: {e}"
            self.logger.error(error_msg)
            raise ConnectionError(error_msg)

    def execute_query(self, sql):
        with self.db_engine.connect() as conn:
            self.logger.info(f"Executing query: {sql}")
            start_time = time.time()
            results = conn.execute(text(sql))
            end_time = time.time()
            self.logger.info(f"Query Execution Time: {(end_time - start_time):.4f} sec")
        for row in results:
            print(row)
