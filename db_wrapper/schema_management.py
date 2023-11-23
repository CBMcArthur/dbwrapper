from libraries import logging_utils
try:
    from query_execution import execute_query
except ImportError:
    from db_wrapper.query_execution import execute_query

logger = logging_utils.get_logger(__name__)


def schema_exists(db_engine, schema_name):
    """
    Check to see if the specified schema already exists
    :param db_engine:
    :param schema_name:
    :return:
    """
    sql = f"SELECT 1 exists FROM information_schema.schemata WHERE schema_name = '{schema_name}'"
    result = execute_query(db_engine, sql)
    if len(result) > 0:
        return bool(result[0])
    else:
        return False


def create_schema(db_engine, schema_name):
    """
    Create a new schema in the db_engine's PostgreSQL database
    :param db_engine:
    :param schema_name:
    :return:
    """
    if schema_exists(db_engine, schema_name):
        error_msg = f"Error: The schema specified, {schema_name}, already exists. Creation skipped."
        logger.warning(error_msg)
        return error_msg

    sql = f"CREATE SCHEMA IF NOT EXISTS {schema_name}"
    result = execute_query(db_engine, sql)
    return result


def rename_schema(db_engine, old_name, new_name):
    """
    Alter an existing schema to rename it.
    :param db_engine:
    :param old_name:
    :param new_name:
    :return:
    """
    if not schema_exists(db_engine, old_name):
        error_msg = f"Error: The schema specified, {old_name}, does not exist.  Renaming skipped."
        logger.error(error_msg)
        return error_msg
    if schema_exists(db_engine, new_name):
        error_msg = f"Error: The new schema name, {new_name}, already exists. Renaming skipped."
        logger.error(error_msg)
        return error_msg

    sql = f"ALTER SCHEMA {old_name} RENAME TO {new_name}"
    result = execute_query(db_engine, sql)
    return result

def delete_schema(db_engine, schema_name, cascade=True):
    """
    Delete the specified schema
    :param db_engine:
    :param schema_name: Schema to delete
    :param cascade: Also delete any tables or other objects in the schema
    :return:
    """
    if not schema_exists(db_engine, schema_name):
        error_msg = f"Error: The schema specified, {schema_name}, does not exist.  Cannot delete."
        logger.error(error_msg)
        return error_msg

    sql = f"DROP SCHEMA IF EXISTS {schema_name} {'CASCADE' if cascade else ''}"
    result = execute_query(db_engine, sql)
    return result
