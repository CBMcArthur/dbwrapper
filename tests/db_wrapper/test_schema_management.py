import pytest

from unittest.mock import Mock, patch
from db_wrapper.exceptions import DatabaseErrorException, DatabaseWarningException
from db_wrapper import schema_management


class TestSchemaManagement:
    @pytest.fixture
    def dbwrapper(self):
        return Mock()

    @pytest.mark.parametrize('schema_name, expected_result', [
        ("existing_schema", True),  # Test when schema exists
        ("nonexisting_schema", False),  # Test when schema does not exist
    ])
    def test_schema_exists(self, dbwrapper, schema_name, expected_result):
        dbwrapper.execute_query.return_value = [{'exists': True}] if expected_result is True else [{'exists': False}]
        result = schema_management.schema_exists(dbwrapper, schema_name)
        assert result == expected_result
        dbwrapper.execute_query.assert_called_once_with('SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = :schema_name)', schema_name=schema_name)

    def test_schema_exists_exception(self, dbwrapper):
        dbwrapper.execute_query.side_effect = Exception("Test error")
        with pytest.raises(DatabaseErrorException):
            schema_management.schema_exists(dbwrapper, 'some schema')

    def test_schema_exists_class(self, dbwrapper):
        with patch('db_wrapper.schema_management.schema_exists') as mock_schema_exists:
            schema_manager = schema_management.SchemaManager(dbwrapper, 'test_schema')

            mock_schema_exists.side_effect = [True, False, DatabaseErrorException('Error checking schema')]

            assert schema_manager.schema_exists() is True
            assert schema_manager.schema_exists() is False
            assert schema_manager.schema_exists() is False

            mock_schema_exists.assert_called_with(dbwrapper=dbwrapper, schema_name='test_schema')


    @pytest.mark.parametrize('schema_name, has_objects, expected_result', [
        ("schema_with_objects", True, True),
        ("schema_without_objects", False, False)
    ])
    def test_schema_has_objects(self, dbwrapper, schema_name, has_objects, expected_result):
        dbwrapper.execute_query.return_value = [{'exists': True}] if expected_result is True else [{'exists': False}]
        result = schema_management.schema_has_objects(dbwrapper, schema_name)
        assert result == expected_result
        dbwrapper.execute_query.assert_called_once_with("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = :schema_name LIMIT 1);", schema_name=schema_name)

    def test_schema_has_objects_exception(self, dbwrapper):
        dbwrapper.execute_query.side_effect = Exception("Test error")
        with pytest.raises(DatabaseErrorException):
            schema_management.schema_has_objects(dbwrapper, 'some schema')

    def test_schema_has_objects_class(self, dbwrapper):
        with patch('db_wrapper.schema_management.schema_has_objects') as mock_schema_has_objects:
            schema_manager = schema_management.SchemaManager(dbwrapper, 'test_schema')

            mock_schema_has_objects.side_effect = [True, False, DatabaseErrorException("Error checking schema")]
            assert schema_manager.schema_has_objects() is True
            assert schema_manager.schema_has_objects() is False
            assert schema_manager.schema_has_objects() is False

            mock_schema_has_objects.assert_called_with(dbwrapper=dbwrapper, schema_name='test_schema')

    @pytest.mark.parametrize("schema_name, recreate, cascade, schema_exists, objects_exists, expected_warning, expected_error", [
        ('existing schema', False, True, True, True, DatabaseWarningException, None),  # Existing schema with object, but don't recreate = Warning
        ('existing schema', True, False, True, True, None, DatabaseErrorException),  # Existing schema with object, recreate without cascade = warning
        ('existing schema', True, True, True, True, None, None),  # Existing schema with object, recreate with cascade = success
        ('existing schema', True, True, True, False, None, None),  # Existing schema without object, recreate = success
        ('existing schema', True, False, True, False, None, None),  # Existing schema without object, recreate = success
        ('nonexisting', False, True, False, False, None, None)  # Nonexisting schema create = success
    ])
    @patch('db_wrapper.schema_management.schema_exists')
    @patch('db_wrapper.schema_management.delete_schema')
    def test_create_schema(self, mock_delete_schema, mock_schema_exists, dbwrapper,
                           schema_name, recreate, cascade, schema_exists, objects_exists, expected_warning, expected_error):
        mock_schema_exists.return_value = schema_exists
        dbwrapper.execute_query.return_value = True if expected_warning is None else False

        if schema_exists and objects_exists and recreate and not cascade:
            mock_delete_schema.side_effect = DatabaseErrorException("Error deleting schema")

        if expected_warning:
            with pytest.raises(expected_warning):
                schema_management.create_schema(dbwrapper, schema_name, recreate, cascade)
        elif expected_error:
            with pytest.raises(expected_error):
                schema_management.create_schema(dbwrapper, schema_name, recreate, cascade)
        else:
            result = schema_management.create_schema(dbwrapper, schema_name, recreate, cascade)
            assert result is True
            dbwrapper.execute_query.assert_called_once_with(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    @patch('db_wrapper.schema_management.schema_exists')
    @patch('db_wrapper.schema_management.delete_schema')
    def test_create_schema_exception(self, mock_delete_schema, mock_schema_exists, dbwrapper):
        mock_schema_exists.return_value = False
        dbwrapper.execute_query.side_effect = Exception("Test error")
        with pytest.raises(DatabaseErrorException):
            schema_management.create_schema(dbwrapper, 'some schema', True, True)

    def test_create_schema_class(self, dbwrapper):
        with patch('db_wrapper.schema_management.create_schema') as mock_create_schema:
            schema_manager = schema_management.SchemaManager(dbwrapper, 'test_schema')

            mock_create_schema.side_effect = [True, False, DatabaseErrorException("Error creating"), DatabaseWarningException("Warning creating")]
            assert schema_manager.create_schema() is True
            assert schema_manager.create_schema() is False
            assert schema_manager.create_schema() is False
            assert schema_manager.create_schema() is False

            mock_create_schema.assert_called_with(dbwrapper=dbwrapper, schema_name='test_schema', recreate=False, cascade=True)


    @pytest.mark.parametrize("schema_name, new_name, exception_thrown", [
        ('missing_schema', 'renamed_schema', DatabaseErrorException),  # DatabaseErrorException: renaming a schema that doesn't exist
        ('schema_name', 'already_used', DatabaseErrorException),  # DatabaseErrorException: new name is already in use
        ('schema_name', "renamed_schema", None)
    ])
    @patch('db_wrapper.schema_management.schema_exists')
    def test_rename_schema(self, mock_schema_exists, dbwrapper, schema_name, new_name, exception_thrown):
        captured_schema_name = None
        def schema_exists_side_effect(self, name):
            nonlocal captured_schema_name
            captured_schema_name = name
            return False if name == 'missing_schema' or name == 'renamed_schema' else True
        mock_schema_exists.side_effect = schema_exists_side_effect
        dbwrapper.execute_query.side_effect = [
            DatabaseErrorException("Error renaming schema") if exception_thrown == DatabaseErrorException else True
        ]

        if exception_thrown:
            with pytest.raises(exception_thrown):
                schema_management.rename_schema(dbwrapper, schema_name, new_name)
        else:
            result = schema_management.rename_schema(dbwrapper, schema_name, new_name)
            assert result is True
            dbwrapper.execute_query.assert_called_once_with(f"ALTER SCHEMA {schema_name} RENAME TO {new_name}")

    def test_rename_schema_class(self, dbwrapper):
        with patch('db_wrapper.schema_management.rename_schema') as mock_rename_schema:
            schema_manager = schema_management.SchemaManager(dbwrapper, 'test_schema')
            mock_rename_schema.side_effect = [True]
            assert schema_manager.rename_schema('new_name') is True
            assert schema_manager.schema_name == 'new_name'

            schema_manager = schema_management.SchemaManager(dbwrapper, 'test_schema')
            mock_rename_schema.side_effect = [False]
            assert schema_manager.rename_schema('new_name') is False

            schema_manager = schema_management.SchemaManager(dbwrapper, 'test_schema')
            mock_rename_schema.side_effect = [DatabaseErrorException('Testing error')]
            assert schema_manager.rename_schema('new_name') is False

            schema_manager = schema_management.SchemaManager(dbwrapper, 'test_schema')
            mock_rename_schema.side_effect = [DatabaseWarningException('Testing Warning')]
            assert schema_manager.rename_schema('new_name') is False

            mock_rename_schema.assert_called_with(dbwrapper=schema_manager.dbwrapper, old_name='test_schema', new_name='new_name')


    @pytest.mark.parametrize("schema_name, cascade, schema_exists, has_objects, expected_exception", [
        ('existing', False, True, False, None),  # Existing with no objects = success
        ('existing', False, True, True, DatabaseErrorException),  # Existing with objects, no cascade = error
        ('existing', True, True, True, None),  # Existing with objects, with cascade = success
        ('nonexisting', False, False, False, DatabaseWarningException),  # Non-existing schema = warning
    ])
    @patch('db_wrapper.schema_management.schema_exists')
    @patch('db_wrapper.schema_management.schema_has_objects')
    def test_delete_schema(self, mock_schema_has_objects, mock_schema_exists, dbwrapper,
                           schema_name, cascade, schema_exists, has_objects, expected_exception):
        mock_schema_has_objects.return_value = has_objects
        mock_schema_exists.return_value = schema_exists
        dbwrapper.execute_query.return_value = True if expected_exception is None else False

        if expected_exception:
            with pytest.raises(expected_exception):
                schema_management.delete_schema(dbwrapper, schema_name, cascade)
        else:
            result = schema_management.delete_schema(dbwrapper, schema_name, cascade)
            assert result is True
            dbwrapper.execute_query.assert_called_once_with(f"DROP SCHEMA IF EXISTS {schema_name} {'CASCADE' if cascade else ''}")

    def test_delete_schema_class(self, dbwrapper):
        with patch('db_wrapper.schema_management.delete_schema') as mock_delete_schema:
            schema_manager = schema_management.SchemaManager(dbwrapper, 'test_schema')
            mock_delete_schema.side_effect = [False, DatabaseErrorException('Test Error'), DatabaseWarningException('Test Warning'), True]

            assert schema_manager.delete_schema() is False
            assert schema_manager.delete_schema() is False
            assert schema_manager.delete_schema() is False
            assert schema_manager.delete_schema() is True

            mock_delete_schema.assert_called_with(dbwrapper=dbwrapper, schema_name='test_schema', cascade=False)
