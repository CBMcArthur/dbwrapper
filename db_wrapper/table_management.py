from sqlalchemy import MetaData, Table
from libraries import logging_utils
from db_wrapper.exceptions import DatabaseErrorException, DatabaseWarningException
from db_wrapper.schema_management import schema_exists, create_schema

logger = logging_utils.get_logger(__name__)


class TableManager:
    def __init__(self, dbwrapper=None, schema_name=None, table_name=None):
        logging_utils.configure_logging()
        self.logger = logging_utils.get_logger(__name__)
        self.dbwrapper = dbwrapper
        self.schema_name = schema_name
        self.table_name = table_name


    def table_exists(self):
        try:
            exists = table_exists(self.dbwrapper, self.schema_name, self.table_name)
            return exists
        except DatabaseErrorException as e:
            self.logger.error(str(e))
        return False

    def delete_table(self):
        try:
            deleted = delete_table(self.dbwrapper, self.schema_name, self.table_name)
            return deleted
        except DatabaseErrorException as e:
            self.logger.error(str(e))
        except DatabaseWarningException as e:
            self.logger.warning(str(e))
        return False

    def create_table(self, columns=None, recreate=False):
        try:
            response = create_table(self.dbwrapper, self.schema_name, self.table_name, columns=columns,
                                    recreate=recreate, dry_run=self.dbwrapper.dry_run)
            return response
        except DatabaseErrorException as e:
            self.logger.error(str(e))
        except DatabaseWarningException as e:
            self.logger.warning(str(e))
        except ValueError as e:
            self.logger.error(str(e))
        return False

    def rename_table(self, new_schema, new_name, replace_new=False):
        try:
            response = rename_table(self.dbwrapper, self.schema_name, self.table_name, new_schema, new_name, replace_new)
            if response is True:
                self.schema_name = new_schema
                self.table_name = new_name
            return response
        except ValueError as e:
            self.logger.error(str(e))
        except DatabaseErrorException as e:
            self.logger.error(str(e))
        except DatabaseWarningException as e:
            self.logger.warning(str(e))
        return False

    def describe_table(self):
        try:
            return describe_table(self.dbwrapper, self.schema_name, self.table_name)
        except ValueError as e:
            self.logger.error(str(e))
        return None

    def insert_list(self, values):
        try:
            response = insert_list(self.dbwrapper, self.schema_name, self.table_name, values)
            return response
        except DatabaseErrorException as e:
            self.logger.error(str(e))
        except ValueError as e:
            self.logger.error(str(e))
        return False


def table_exists(dbwrapper, schema_name, table_name):
    """
    Check to see if the specified table already exists
    :param dbwrapper:
    :param schema_name:
    :param table_name:
    :return:
    """
    sql = f"""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = :schema_name
                AND table_name = :table_name
        );
    """
    try:
        result = dbwrapper.execute_query(sql, {'schema_name': schema_name, 'table_name': table_name})
        return bool(result[0]['exists'])
    except Exception as e:
        error_msg = f"Error checking if table, {schema_name}.{table_name}, exists: {e}"
        raise DatabaseErrorException(error_msg, sql)

def delete_table(dbwrapper, schema_name, table_name, dry_run=False):
    if not schema_exists(dbwrapper, schema_name):
        error_msg = f"The schema, {schema_name}, does not exist. Cannot delete tables from it."
        raise DatabaseWarningException(error_msg)
    if not table_exists(dbwrapper, schema_name, table_name):
        error_msg = f"The table, {schema_name}.{table_name}, does not exist.  Cannot delete it."
        raise DatabaseWarningException(error_msg)

    sql = f"DROP TABLE IF EXISTS {schema_name}.{table_name}"
    if not dry_run:
        result = dbwrapper.execute_query(sql)
        print(result)
        return result
    else:
        print(f"[DRY RUN] {dbwrapper.format_query(sql)}")
        return True


def create_table(dbwrapper, schema_name, table_name, columns, recreate=False, dry_run=False):
    if not isinstance(columns, list) or not all(isinstance(col, str) or isinstance(col, dict) for col in columns):
        error_msg = "Columns provided must be a list of strings of column names or list of dicts of column definitions"
        raise ValueError(error_msg)

    if table_exists(dbwrapper, schema_name, table_name):
        if not recreate:
            error_msg = f"The table, {schema_name}.{table_name}, already exists"
            raise DatabaseErrorException(error_msg)
        else:
            deleted = delete_table(dbwrapper, schema_name, table_name, dry_run)
            if not deleted:
                error_msg = f"The table, {schema_name}.{table_name}, could not be deleted to be recreated."
                raise DatabaseErrorException(error_msg)

    if not schema_exists(dbwrapper, schema_name):
        created = create_schema(dbwrapper, schema_name)
        if not created:
            error_msg = f"Problem creating schema {schema_name}"
            raise DatabaseErrorException(error_msg)

    # Build sql (or ORM??) for create table schema.table with columns
    temp_defs = []
    for col in columns:
        if isinstance(col, str):
            temp_defs.append({'col_name': col, 'type': 'text', 'default': 'NULL'})
        elif isinstance(col, dict):
            temp_defs.append(col)

    column_defs = []
    for col in temp_defs:
        if col.get('col_name') is None:
            error_msg = "Column definitions must include a 'col_name' key"
            raise ValueError(error_msg)
        column_defs.append((f"{col['col_name']} "
                             f"{col.get('type') if col.get('type') is not None else 'text'} "
                             f"{'DEFAULT ' if col.get('default') is None or 'DEFAULT' not in col.get('default').upper() else ''}"
                             f"{col.get('default') if col.get('default') is not None else 'NULL'}"))

    sql = f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} ({', '.join(column_defs)})"

    if not dry_run:
        result = dbwrapper.execute_query(sql)
        return result
    else:
        print(f"[DRY RUN] {dbwrapper.format_query(sql)}")
        return True

def rename_table(dbwrapper, old_schema, old_name, new_schema, new_name, replace_new=False, dry_run=False):
    if not table_exists(dbwrapper, old_schema, old_name):
        error_msg = f"The table, {old_schema}.{old_name}, does not exist"
        raise ValueError(error_msg)

    if table_exists(dbwrapper, new_schema, new_name):
        if replace_new is False:
            error_msg = f"A table with that name, {new_schema}.{new_name} already exists"
            raise DatabaseErrorException(error_msg)
        delete_table(dbwrapper, new_schema, new_name)

    if not schema_exists(dbwrapper, new_schema):
        create_schema(dbwrapper, new_schema)

    sql = f"ALTER TABLE {old_schema}.{old_name} RENAME TO {new_schema}.{new_name}"

    if not dry_run:
        results = dbwrapper.execute_query(sql)
        return results
    else:
        print(f"[DRY RUN] {dbwrapper.format_query(sql)}")
        return True


def describe_table(dbwrapper, schema_name, table_name):
    if not table_exists(dbwrapper, schema_name, table_name):
        raise ValueError(f"The table, {schema_name}.{table_name}, does not exist")

    sql = "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = :schema_name AND table_name = :table_name"
    results = dbwrapper.execute_query(sql, {'schema_name': schema_name, 'table_name': table_name})
    return results



def validate_values(values, table_columns=None):
    if not isinstance(values, list):
        raise ValueError("Values to insert must be a list")
    if not values:
        raise ValueError("Must include at least one set of values to insert")
    if not all(isinstance(row, dict) for row in values):
        raise ValueError("All elements of values must be a dict with a row to insert")

    first_dict_keys = set(values[0].keys())
    for d in values:
        if set(d.keys()) != first_dict_keys:
            raise ValueError("All elements of values must contain the same set of keys/columns")
        if table_columns is not None and not set(d.keys()).issubset(set([col['column_name'] for col in table_columns])):
            raise ValueError("Values to insert contain columns that do not exist")
    return True

def insert_list(dbwrapper, schema_name, table_name, values, dry_run=False):
    validate_values(values, describe_table(dbwrapper, schema_name, table_name))
    if not table_exists(dbwrapper, schema_name, table_name):
        raise DatabaseErrorException(f"Table, {schema_name}.{table_name}, does not exist.")

    cols = values[0].keys()
    sql = f"INSERT INTO {schema_name}.{table_name} ({', '.join(cols)}) VALUES (:{', :'.join(cols)})"

    if not dry_run:
        result = dbwrapper.execute_query(sql, values)
        return result
    else:
        print(f"[DRY RUN] {sql}")
        return True
