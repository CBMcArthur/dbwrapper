import pytest

from unittest.mock import patch, Mock
from _pytest.monkeypatch import MonkeyPatch
from dotenv import dotenv_values, find_dotenv
from sqlalchemy.exc import OperationalError
from db_wrapper.connection import DatabaseConnection

env_values = dotenv_values(find_dotenv())

@pytest.fixture
def set_environment_variables(monkeypatch: MonkeyPatch):
    def set_variables(host, port, user, password, database):
        if host is not None:
            monkeypatch.setenv('DB_HOST', host)
        if port is not None:
            monkeypatch.setenv('DB_PORT', port)
        if user is not None:
            monkeypatch.setenv('DB_USER', user)
        if password is not None:
            monkeypatch.setenv('DB_PASSWORD', password)
        if database is not None:
            monkeypatch.setenv('DB_DATABASE', database)
    return set_variables

@pytest.fixture
def db_connection():
    with patch('db_wrapper.DatabaseConnection.validate_parameters'):
        with patch('db_wrapper.connection.create_engine'):
            yield

class TestDatabaseConnection:

    @pytest.mark.parametrize("host, port, user, password, database, expected_error", [
        ('localhost', 5432, 'user', 'pass', 'db', None),  # Valid parameters
        (None, 5432, 'user', 'pass', 'db', "Connection Creation Error"),  # Missing host
        ('localhost', 'invalid port', 'user', 'pass', 'db', 'An invalid port'),  # Invalid port
        ('invalid host', 5432, 'user', 'pass', 'db', 'An invalid hostname'),  # Invalid host
    ])
    def test_validate_parameters(self, host, port, user, password, database, expected_error, set_environment_variables):
        set_environment_variables(host, port, user, password, database)

        db_connection = DatabaseConnection()

        try:
            db_connection.validate_parameters()
            error_msg = None
        except ValueError as e:
            error_msg = str(e)

        if expected_error is not None:
            assert error_msg is not None
            assert expected_error in error_msg
        else:
            assert error_msg is None


    def test_create_db_engine(self, db_connection, set_environment_variables):
        set_environment_variables('localhost', 5432, 'root', 'root', 'postgres')
        db_connection = DatabaseConnection()
        db_engine = db_connection.create_db_engine()

        db_connection.validate_parameters.assert_called_once()
        db_engine.connect.assert_called_once()

        assert db_connection.validate_parameters.call_count == 1, 'validate_parameters() should be called once'
        assert db_engine.connect.call_count == 1, 'engine.connect should be called once'

    def test_create_db_engine_twice(self, db_connection, set_environment_variables):
        set_environment_variables('localhost', 5432, 'root', 'root', 'postgres')
        db_connection = DatabaseConnection()

        engine1 = db_connection.create_db_engine()
        engine2 = db_connection.create_db_engine()

        assert engine1 == engine2
        db_connection.validate_parameters.assert_called_once()
        engine1.connect.assert_called_once()

    @patch('db_wrapper.connection.create_engine', side_effect=lambda *args, **kwargs: Mock(connect=Mock(side_effect=OperationalError('Mocked Error', params={}, orig=None))))
    def test_create_db_engine_failure(self, create_engine_mock, db_connection, set_environment_variables):
        set_environment_variables('bad_host', 5432, 'root', 'root', 'postgres')
        db_connection = DatabaseConnection()
        with pytest.raises(ConnectionError) as exc_info:
            db_connection.create_db_engine()

        assert "Failed to create the database engine" in str(exc_info.value)
        db_connection.validate_parameters.assert_called_once()
        create_engine_mock.assert_called_once()
