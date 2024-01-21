from libraries import logging_utils
from sqlalchemy.exc import InternalError
from db_wrapper.exceptions import DatabaseErrorException, DatabaseWarningException

logger = logging_utils.get_logger(__name__)


class SchemaManager:
    def __init__(self, dbwrapper=None, schema_name=None):
        logging_utils.configure_logging()
        self.logger = logging_utils.get_logger(__name__)
        self.dbwrapper = dbwrapper
        self.schema_name = schema_name

    def schema_exists(self):
        try:
            exists = schema_exists(dbwrapper=self.dbwrapper, schema_name=self.schema_name)
        except DatabaseErrorException as e:
            self.logger.error(str(e))
            return False
        return exists

    def schema_has_objects(self):
        try:
            has_objects = schema_has_objects(dbwrapper=self.dbwrapper, schema_name=self.schema_name)
        except DatabaseErrorException as e:
            self.logger.error(str(e))
            return False
        return has_objects

    def create_schema(self, recreate=False, cascade=True):
        try:
            response = create_schema(dbwrapper=self.dbwrapper, schema_name=self.schema_name, recreate=recreate, cascade=cascade)
            return response
        except DatabaseErrorException as e:
            self.logger.error(str(e))
        except DatabaseWarningException as e:
            self.logger.warning(str(e))
        return False

    def rename_schema(self, new_schema_name):
        try:
            response = rename_schema(dbwrapper=self.dbwrapper, old_name=self.schema_name, new_name=new_schema_name)
        except DatabaseErrorException as e:
            self.logger.error(str(e))
            return False
        except DatabaseWarningException as e:
            self.logger.warning(str(e))
            return False
        if response is True:
            self.schema_name = new_schema_name
        return response

    def delete_schema(self, cascade=False):
        try:
            response = delete_schema(dbwrapper=self.dbwrapper, schema_name=self.schema_name, cascade=cascade)
        except DatabaseErrorException as e:
            self.logger.error(str(e))
            return False
        except DatabaseWarningException as e:
            self.logger.error(str(e))
            return False
        return response


def schema_exists(dbwrapper, schema_name):
    """
    Check to see if the specified schema already exists
    :param dbwrapper: Database wrapper object with an execute_query method
    :param schema_name: Name of the schema to check
    :return: True if the schema exists, False otherwise
    """
    sql = "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = :schema_name)"
    try:
        result = dbwrapper.execute_query(sql, schema_name=schema_name)
        return bool(result[0]['exists'])
    except Exception as e:
        error_msg = f"Error checking if schema, {schema_name}, exists: {e}"
        raise DatabaseErrorException(error_msg, sql)


def schema_has_objects(dbwrapper, schema_name):
    """
    Check to see if schema has any tables
    :param dbwrapper:
    :param schema_name:
    :return:
    """
    sql = f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = :schema_name LIMIT 1);"
    try:
        result = dbwrapper.execute_query(sql, schema_name=schema_name)
        return bool(result[0]['exists'])
    except Exception as e:
        error_msg = f"Error checking if schema, {schema_name}, has objects: {e}"
        raise DatabaseErrorException(error_msg, sql)


def create_schema(dbwrapper, schema_name, recreate=False, cascade=True):
    """
    Create a new schema in the db_engine's PostgreSQL database
    :param dbwrapper:
    :param schema_name:
    :param recreate:
    :param cascade:
    :return:
    """
    if schema_exists(dbwrapper, schema_name):
        if not recreate:
            error_msg = f"The schema specified, {schema_name}, already exists. Creation skipped."
            raise DatabaseWarningException(error_msg)
        delete_schema(dbwrapper, schema_name, cascade)

    sql = f"CREATE SCHEMA IF NOT EXISTS {schema_name}"
    try:
        result = dbwrapper.execute_query(sql)
        return result
    except Exception as e:
        error_msg = f"Error creating schema, {schema_name}: {e}"
        raise DatabaseErrorException(error_msg, sql)


def rename_schema(dbwrapper, old_name, new_name):
    """
    Alter an existing schema to rename it.
    :param dbwrapper:
    :param old_name:
    :param new_name:
    :return:
    """
    if not schema_exists(dbwrapper, old_name):
        error_msg = f"The schema, {old_name}, does not exist.  Renaming skipped."
        raise DatabaseErrorException(error_msg)
    if schema_exists(dbwrapper, new_name):
        error_msg = f"The new schema, {new_name}, already exists. Renaming skipped."
        raise DatabaseErrorException(error_msg)

    sql = f"ALTER SCHEMA {old_name} RENAME TO {new_name}"
    result = dbwrapper.execute_query(sql)
    return result

def delete_schema(dbwrapper, schema_name, cascade=False):
    """
    Delete the specified schema
    :param dbwrapper:
    :param schema_name: Schema to delete
    :param cascade: Also delete any tables or other objects in the schema
    :return:
    """
    if not schema_exists(dbwrapper, schema_name):
        error_msg = f"The schema, {schema_name}, does not exist.  Cannot delete."
        raise DatabaseWarningException(error_msg)

    if cascade is False:
        if schema_has_objects(dbwrapper, schema_name):
            error_msg = f"The schema, {schema_name}, cannot be deleted when tables exist in it unless cascade is True"
            raise DatabaseErrorException(error_msg)

    sql = f"DROP SCHEMA IF EXISTS {schema_name} {'CASCADE' if cascade else ''}"
    try:
        result = dbwrapper.execute_query(sql)
        return result
    except InternalError as e:
        if 'DependentObjectsStillExist' in str(e):
            error_msg = f"The schema, {schema_name}, has tables or other objects in it. Either drop the objects or use the cascade option"
            raise DatabaseErrorException(error_msg)
        else:
            error_msg = f"A problem occurred dropping the schema, {schema_name}"
            raise DatabaseErrorException(error_msg)
