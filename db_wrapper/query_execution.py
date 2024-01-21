import time
import sqlparse

from sqlalchemy import text, TextClause
from sqlalchemy.exc import ProgrammingError
from libraries import logging_utils

logger = logging_utils.get_logger(__name__)


def execute_query(connection, sql, params=None, dry_run=False):
    """
    Execute a SQL query using the provided DatabaseConnection instance
    :param connection: connection to the database via connection module
    :param sql: SQL query to execute
    :param params: Dict of key-values for parametrizing queries
    :param dry_run: False to execute the SQL, True to only print the formatted SQL
    :return:
    """
    parsed_sql = validate_sql(sql)
    query = text(parsed_sql[0].normalized)
    query_type = get_query_type(parsed_sql)

    try:
        connection.get_db_engine()
    except ConnectionError as e:
        error_msg = f"Unable to connect to the database: {str(e)}"
        logger.error(error_msg)
        return False

    start_time = time.time()
    if not dry_run:
        with connection.get_db_engine().connect() as conn:
            try:
                results = conn.execute(query, params)
                conn.commit()
            except ProgrammingError as e:
                error_msg = f"An error occurred executing the query: {e.orig}"
                logger.error(error_msg)
                return False

            result_data = prep_results(results, query_type)
    else:
        if params is not None:
            compiled_sql = query.bindparams(**params).compile(compile_kwargs={'literal_binds': True})
        else:
            compiled_sql = query.compile()

        result_data = f"[DRY RUN] {format_sql(str(compiled_sql))}"
        print(result_data)
        logger.debug(result_data)

    end_time = time.time()
    logger.info(f'Query execution time {(end_time - start_time):.4f} sec')
    return result_data


def prep_results(results, query_type):
    if query_type == 'SELECT':
        return [row for row in results.mappings()]
    elif query_type in ['INSERT', 'UPDATE', 'DELETE']:
        return results.rowcount
    elif query_type in ['CREATE', 'DROP', 'ALTER', 'DO']:
        return True
    else:
        return results


def format_sql(sql):
    parsed_sql = validate_sql(sql)
    return sqlparse.format(parsed_sql[0].normalized, keyword_case='upper')

def get_query_type(sql):
    parsed_sql = validate_sql(sql)
    return parsed_sql[0].get_type()

def validate_sql(sql):
    if isinstance(sql, tuple) and all(isinstance(stmt, sqlparse.sql.Statement) for stmt in sql):
        return sql
    elif isinstance(sql, TextClause):
        sql = str(sql)
    elif not isinstance(sql, str):
        error_msg = f"Unknown type for sql {type(sql)}. Allowed type are string and TextClause"
        logger.error(error_msg)
        raise ValueError(error_msg)

    parsed_sql = sqlparse.parse(sql)
    if parsed_sql[0].get_type() == 'UNKNOWN':
        error_msg = f"SQL provided appears to be malformed: {sql}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    return parsed_sql
