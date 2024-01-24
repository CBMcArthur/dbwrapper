import pytest

from unittest.mock import Mock, patch
from db_wrapper.exceptions import DatabaseErrorException, DatabaseWarningException
from db_wrapper import table_management

class TestTableManagement:
    @pytest.fixture
    def dbwrapper(self):
        return Mock()

    @pytest.mark.parametrize('schema_name, table_name, expected_result', [
        ('nonexisting_schema', 'this_table', False),  # Non existing schema = False
        ('this_schema', 'nonexisting_table', False),  # existing schema, nonexsiting table = False
        ('this_schema', 'this_table', True)  # Existing schema, existing table = True
    ])
    def test_table_exists(self, dbwrapper, schema_name, table_name, expected_result):
        dbwrapper.execute_query.return_value = [{'exists': True}] if expected_result is True else [{'exists': False}]
        result = table_management.table_exists(dbwrapper, schema_name, table_name)

        assert result == expected_result

    def test_table_exists_exception(self, dbwrapper):
        dbwrapper.execute_query.side_effect = Exception('test exception')
        with pytest.raises(DatabaseErrorException):
            table_management.table_exists(dbwrapper, 'this_schema', 'this_table')

    def test_table_exists_class(self, dbwrapper):
        with patch('db_wrapper.table_management.table_exists') as mock_table_exists:
            table_manager = table_management.TableManager(dbwrapper, 'this_schema', 'this_table')

            mock_table_exists.side_effect = [True, False, DatabaseErrorException('Error checking table')]

            assert table_manager.table_exists() is True
            assert table_manager.table_exists() is False
            assert table_manager.table_exists() is False

            mock_table_exists.assert_called_with(dbwrapper, 'this_schema', 'this_table')


    @pytest.mark.parametrize('schema_name, table_name, schema_exists, table_exists, expected_result, expected_exception', [
       ('nonexisting_schema', 'this_table', False, True, None, DatabaseWarningException),  # Nonexisting schema = Warning
       ('this_schema', 'nonexisting_table', True, False, None, DatabaseWarningException),  # Existing schema, nonexisting table = Warning
       ('this_schema', 'this_table', True, True, True, None)  # Existing schema & table = True
    ])
    @patch('db_wrapper.table_management.schema_exists')
    @patch('db_wrapper.table_management.table_exists')
    def test_delete_table(self, mock_table_exists, mock_schema_exists, dbwrapper,
                          schema_name, table_name, schema_exists, table_exists, expected_result, expected_exception):
        mock_schema_exists.return_value = schema_exists
        mock_table_exists.return_value = table_exists
        dbwrapper.execute_query.return_value = expected_result

        if expected_exception:
            with pytest.raises(expected_exception):
                table_management.delete_table(dbwrapper, schema_name, table_name)
        else:
            result = table_management.delete_table(dbwrapper, schema_name, table_name)
            assert result == expected_result
            dbwrapper.execute_query.assert_called_once_with(f"DROP TABLE IF EXISTS {schema_name}.{table_name}")

    def test_delete_table_exception(self, dbwrapper):
        dbwrapper.execute_query.side_effect = DatabaseErrorException('test exception')
        with pytest.raises(DatabaseErrorException):
            table_management.delete_table(dbwrapper, 'this_schema', 'this_table')

    def test_delete_table_class(self, dbwrapper):
        with patch('db_wrapper.table_management.delete_table') as mock_delete_table:
            table_manager = table_management.TableManager(dbwrapper, 'this_schema', 'this_table')

            mock_delete_table.side_effect = [True, False, DatabaseWarningException('Error checking table or schema'), DatabaseErrorException('test exception')]

            assert table_manager.delete_table() is True
            assert table_manager.delete_table() is False
            assert table_manager.delete_table() is False
            assert table_manager.delete_table() is False

            mock_delete_table.assert_called_with(dbwrapper, 'this_schema', 'this_table')


    columns = [{'col_name': 'id', 'type': 'serial'}, 'first_name', {'col_name': 'username', 'type': 'varchar(15)', 'default': 'not null'}]
    @pytest.mark.parametrize('schema_name, table_name, columns, recreate, table_exists, schema_exists, expected_results, expected_exception', [
        ('this_schema', 'this_table', 'Columns', False, True, True, None, ValueError),  # Columns is a string = ValueError
        ('this_schema', 'this_table', [1, 2, 4], False, True, True, None, ValueError),  # Columns is list of numbers = ValueError
        ('this_schema', 'this_table', [{'type': 'text', 'default': 'Null'}], False, False, True, None, ValueError),  # Columns dict missing col_name
        ('this_schema', 'this_table', columns, False, True, True, None, DatabaseErrorException),  # Table exists & recreate is False = DatabaseErrorException
        ('this_schema', 'this_table', columns, True, True, True, True, None),  # Table exists & recreate is True = Success
        ('this_schema', 'this_table', columns, False, False, True, True, None),  # Table not exists = Success
        ('this_schema', 'this_table', columns, False, False, False, True, None)  # Schema does not exist = Succes
    ])
    @patch('db_wrapper.table_management.table_exists')
    @patch('db_wrapper.table_management.schema_exists')
    @patch('db_wrapper.table_management.create_schema')
    @patch('db_wrapper.table_management.delete_table')
    def test_create_table(self, mock_delete_table, mock_create_schema, mock_schema_exists, mock_table_exists, dbwrapper,
                          schema_name, table_name, columns, recreate, table_exists, schema_exists, expected_results, expected_exception):
        mock_table_exists.return_value = table_exists
        mock_schema_exists.return_value = schema_exists
        mock_create_schema.return_value = True
        mock_delete_table.return_value = True
        dbwrapper.execute_query.return_value = expected_results

        if expected_exception:
            with pytest.raises(expected_exception):
                table_management.create_table(dbwrapper, schema_name, table_name, columns, recreate)
        else:
            results = table_management.create_table(dbwrapper, schema_name, table_name, columns, recreate)
            assert results == expected_results


    def test_create_table_class(self, dbwrapper):
        dbwrapper.dry_run = False
        with patch('db_wrapper.table_management.create_table') as mock_create_table:
            table_manager = table_management.TableManager(dbwrapper, 'this_schema', 'this_table')

            mock_create_table.side_effect = [True, False, ValueError('Columns not correct'),
                                             DatabaseErrorException('test error'), DatabaseWarningException('test warning')]

            assert table_manager.create_table(['id'], False) is True
            assert table_manager.create_table(['id'], False) is False
            assert table_manager.create_table(['id'], False) is False
            assert table_manager.create_table(['id'], False) is False
            assert table_manager.create_table(['id'], False) is False

            mock_create_table.assert_called_with(dbwrapper, 'this_schema', 'this_table', columns=['id'],
                                                 recreate=False, dry_run=False)


    @pytest.mark.parametrize("old_schema, old_name, new_schema, new_name, replace_new, "
                             "old_table_exists, new_schema_exists, new_table_exists, expected_result, expected_exception", [
        ('this_schema', 'this_table', 'this_schema', 'new_table', False, False, True, False, None, ValueError),  # Old schema.table doesn't exist = ValueError
        ('this_schema', 'this_table', 'this_schema', 'new_table', False, True, True, True, None, DatabaseErrorException),  # New schema.table does exist, replace new is False = DatabaseErrorException
        ('this_schema', 'this_table', 'this_schema', 'new_table', True, True, True, True, True, None),  # New schema.table does exist, replace new is True = Success
        ('this_schema', 'this_table', 'this_schema', 'new_table', False, True, True, False, True, None)  # New schema.table doesn't exist = Success
    ])
    @patch('db_wrapper.table_management.table_exists')
    @patch('db_wrapper.table_management.schema_exists')
    @patch('db_wrapper.table_management.delete_table')
    @patch('db_wrapper.table_management.create_schema')
    def test_rename_table(self, mock_create_schema, mock_delete_schema, mock_schema_exists, mock_table_exists, dbwrapper,
                          old_schema, old_name, new_schema, new_name, replace_new,
                          old_table_exists, new_schema_exists, new_table_exists, expected_result, expected_exception):
        mock_create_schema.return_value = True
        mock_delete_schema.return_value = True
        mock_schema_exists.return_value = new_schema_exists
        mock_table_exists.side_effect = lambda *args, **kwargs: old_table_exists if args[2] == 'this_table' else new_table_exists
        dbwrapper.execute_query.return_value = expected_result

        if expected_exception:
            with pytest.raises(expected_exception):
                table_management.rename_table(dbwrapper, old_schema, old_name, new_schema, new_name, replace_new)
        else:
            result = table_management.rename_table(dbwrapper, old_schema, old_name, new_schema, new_name, replace_new)
            assert result == expected_result
            dbwrapper.execute_query.assert_called_with(f"ALTER TABLE {old_schema}.{old_name} RENAME TO {new_schema}.{new_name}")

    def test_rename_table_class(self, dbwrapper):
        new_schema_name = 'new_schema'
        new_table_name = 'new_table'

        with patch('db_wrapper.table_management.rename_table') as mock_rename_table:
            table_manager = table_management.TableManager(dbwrapper, 'this_schema', 'this_table')
            mock_rename_table.side_effect = [True]
            assert table_manager.rename_table(new_schema_name, new_table_name) is True
            mock_rename_table.assert_called_with(dbwrapper, 'this_schema', 'this_table', new_schema_name, new_table_name, False)
            assert table_manager.schema_name == new_schema_name
            assert table_manager.table_name == new_table_name

            table_manager = table_management.TableManager(dbwrapper, 'this_schema', 'this_table')
            mock_rename_table.side_effect = [False]
            assert table_manager.rename_table(new_schema_name, new_table_name) is False
            mock_rename_table.assert_called_with(dbwrapper, 'this_schema', 'this_table', new_schema_name, new_table_name, False)
            assert table_manager.schema_name == 'this_schema'
            assert table_manager.table_name == 'this_table'


            table_manager = table_management.TableManager(dbwrapper, 'this_schema', 'this_table')
            mock_rename_table.side_effect = [ValueError('Old table does not exist')]
            assert table_manager.rename_table(new_schema_name, new_table_name) is False
            mock_rename_table.assert_called_with(dbwrapper, 'this_schema', 'this_table', new_schema_name, new_table_name, False)
            assert table_manager.schema_name == 'this_schema'
            assert table_manager.table_name == 'this_table'

            table_manager = table_management.TableManager(dbwrapper, 'this_schema', 'this_table')
            mock_rename_table.side_effect = [DatabaseErrorException('test error')]
            assert table_manager.rename_table(new_schema_name, new_table_name) is False
            mock_rename_table.assert_called_with(dbwrapper, 'this_schema', 'this_table', new_schema_name, new_table_name, False)
            assert table_manager.schema_name == 'this_schema'
            assert table_manager.table_name == 'this_table'

            table_manager = table_management.TableManager(dbwrapper, 'this_schema', 'this_table')
            mock_rename_table.side_effect = [DatabaseWarningException('test_warning')]
            assert table_manager.rename_table(new_schema_name, new_table_name) is False
            mock_rename_table.assert_called_with(dbwrapper, 'this_schema', 'this_table', new_schema_name, new_table_name, False)
            assert table_manager.schema_name == 'this_schema'
            assert table_manager.table_name == 'this_table'


    @pytest.mark.parametrize("table_exists", [
        (True),
        (False)
    ])
    @patch('db_wrapper.table_management.table_exists')
    def test_describe_table(self, mock_table_exists, dbwrapper, table_exists):
        table_columns = [{'column_name': 'id', 'data_type': 'serial4'}, {'column_name': 'username', 'data_type': 'text'}]
        sql = "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = :schema_name AND table_name = :table_name"
        values = {'schema_name': 'this_schema', 'table_name': 'this_table'}
        mock_table_exists.return_value = table_exists
        dbwrapper.execute_query.return_value = table_columns
        if not table_exists:
            with pytest.raises(ValueError):
                table_management.describe_table(dbwrapper, 'this_schema', 'this_table')
        else:
            results = table_management.describe_table(dbwrapper, 'this_schema', 'this_table')
            assert results == table_columns
            dbwrapper.execute_query.assert_called_with(sql, values)

    def test_describe_table_class(self, dbwrapper):
        table_columns = [{'column_name': 'id', 'data_type': 'serial4'}, {'column_name': 'username', 'data_type': 'text'}]
        table_manager = table_management.TableManager(dbwrapper, 'this_schema', 'this_table')

        with patch('db_wrapper.table_management.describe_table') as mock_describe_table:
            mock_describe_table.side_effect = [table_columns, ValueError('test exception')]

            assert table_manager.describe_table() == table_columns
            assert table_manager.describe_table() is None

            mock_describe_table.assert_called_with(dbwrapper, 'this_schema', 'this_table')

    @pytest.mark.parametrize("values, table_cols, isvalid, expected_exception", [
        ([{'id': 1, 'username': 'trey'}, {'id': 2, 'username': 'christian'}], None, True, None),  # Valid values
        ([], None, False, "Must include at least one"),  # Empty list
        ("String not a list", None, False, "must be a list"),  # string
        (["String not a dict"], None, False, "must be a dict with a row"),  # list with strings
        ([{'id': 1, 'username': 'trey'}, {'id': 2, 'first_name': 'christian'}], None, False, "must contain the same set of keys"),   # list with dicts & diff keys
        ([{'id': 1, 'username': 'trey'}, {'id': 2, 'first_name': 'christian'}], [{'column_name': 'id'}], False, "columns that do not exist"),  # Mismatched table columns
        ([{'id': 1, 'username': 'trey'}, {'id': 2, 'username': 'christian'}], [{'column_name': 'id'}, {'column_name': 'username'}], True, None)  # Matching table columns
    ])
    def test_validate_value(self, values, table_cols, isvalid, expected_exception):
        if not isvalid:
            with pytest.raises(ValueError) as exc_info:
                table_management.validate_values(values, table_cols)
                assert expected_exception in set(exc_info.value)
        else:
            results = table_management.validate_values(values, table_cols)
            assert results == isvalid

    @pytest.mark.parametrize("table_exists, values_valid, exception_expected, expected_result", [
        (True, False, ValueError, None),  # values is not valid
        (False, True, DatabaseErrorException, None),  # table does not exist
        (True, True, None, True)   # Insert successful
    ])
    @patch('db_wrapper.table_management.validate_values')
    @patch('db_wrapper.table_management.table_exists')
    @patch('db_wrapper.table_management.describe_table')
    def test_insert_list(self, mock_describe_table, mock_table_exists, mock_validate_values, dbwrapper,
                         table_exists, values_valid, exception_expected, expected_result):
        mock_table_exists.return_value = table_exists
        if not table_exists:
            mock_table_exists.side_effect = DatabaseErrorException('test exception')
        else:
            mock_table_exists.side_effect = None
        mock_validate_values.return_value = values_valid
        if not values_valid:
            mock_validate_values.side_effect = ValueError('text exception')
        else:
            mock_validate_values.side_effect = None
        table_columns = [{'column_name': 'id', 'data_type': 'serial4'}, {'column_name': 'username', 'data_type': 'text'}]
        mock_describe_table.return_value = table_columns
        dbwrapper.execute_query.return_value = expected_result
        values = [{'id': 1}, {'id': 2}]

        if exception_expected is not None:
            with pytest.raises(exception_expected):
                table_management.insert_list(dbwrapper, 'this_schema', 'this_table', values)
        else:
            result = table_management.insert_list(dbwrapper, 'this_schema', 'this_table', values)
            assert result == expected_result
            mock_validate_values.assert_called_once_with(values, table_columns)
            mock_table_exists.assert_called_once_with(dbwrapper, 'this_schema', 'this_table')
            dbwrapper.execute_query.assert_called_once_with("INSERT INTO this_schema.this_table (id) VALUES (:id)", values)

    def test_insert_list_class(self, dbwrapper):
        table_manager = table_management.TableManager(dbwrapper, 'this_schema', 'this_table')
        values = [{'id': 1}, {'id': 2}]

        with patch('db_wrapper.table_management.insert_list') as mock_insert_list:
            mock_insert_list.side_effect = [True, ValueError('test exception'), DatabaseErrorException('test exception')]

            assert table_manager.insert_list(values) is True
            assert table_manager.insert_list(values) is False
            assert table_manager.insert_list(values) is False

            mock_insert_list.assert_called_with(dbwrapper, 'this_schema', 'this_table', values)