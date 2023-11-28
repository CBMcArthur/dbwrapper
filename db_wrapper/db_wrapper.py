from libraries import logging_utils
try:
    from connection import DatabaseConnection
    from query_execution import execute_query
except ImportError:
    from db_wrapper.connection import DatabaseConnection
    from db_wrapper.query_execution import execute_query


class DBWrapper:
    def __init__(self, connection=None, host=None, port=None, user=None, password=None, database=None):
        logging_utils.configure_logging()
        self.logger = logging_utils.get_logger(__name__)
        self.db_connection = connection or DatabaseConnection(host, port, user, password, database)


    def execute_query(self, sql=None):
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
        return execute_query(self.db_connection, sql)

    def get_db_connection(self):
        return self.db_connection

    def get_db_engine(self):
        return self.db_connection.get_db_engine()
