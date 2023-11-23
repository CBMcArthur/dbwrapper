import time

from sqlalchemy import text, TextClause
from sqlalchemy.exc import ProgrammingError
from libraries import logging_utils

logger = logging_utils.get_logger(__name__)


def execute_query(db_engine, sql):
    """
    Execute a SQL query using the provided DatabaseConnection instance
    :param db_engine: connection to the database
    :param sql: SQL query to execute
    :return:
    """
    if isinstance(sql, str):
        query = text(sql)
    elif isinstance(sql, TextClause):
        query = sql
    else:
        error_msg = f"Unknown type provided for sql {type(sql)}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    query_type = get_query_type(sql)

    logger.info(f"Executing query: {sql}")
    start_time = time.time()
    with db_engine.connect() as conn:
        try:
            results = conn.execute(query)
            conn.commit()
        except ProgrammingError as e:
            error_msg = f"An error occurred executing the query: {e.orig}"
            logger.error(error_msg)
            return False

        if query_type == 'SELECT':
            result_data = [row for row in results.mappings()]
        elif query_type in ['INSERT', 'UPDATE', 'DELETE']:
            result_data = results.rowcount
        elif query_type in ['CREATE', 'DROP', 'ALTER']:
            result_data = True
        else:
            result_data = results

    end_time = time.time()
    logger.info(f'Query execution time {(end_time - start_time):.4f} sec')
    return result_data


def get_query_type(sql):
    normalized_sql = str(sql).strip().upper()
    return normalized_sql.split(' ')[0]