import dotenv
from libraries import logging_utils
from db_wrapper.connection import DatabaseConnection
from db_wrapper.query_execution import execute_query

dotenv.load_dotenv(dotenv.find_dotenv())

class DBWrapper:
    def __init__(self, dry_run=False):
        logging_utils.configure_logging()
        self.logger = logging_utils.get_logger(__name__)
        self.dry_run = dry_run
        self.db_connection = DatabaseConnection()


    def execute_query(self, sql=None, params=None):
        """
        Execute a specified SQL using the previously created database connection
        :param sql: SQL to execute
        :return:
        """
        if sql is None:
            self.logger.warn("No query was specified.")
            return None
        if self.db_connection is None:
            error_msg = "No database connection has been established."
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        return execute_query(self.db_connection, sql, params, self.dry_run)


    def get_db_connection(self):
        return self.db_connection

    def get_db_engine(self):
        return self.db_connection.get_db_engine()

    def is_dry_run(self):
        return self.dry_run
