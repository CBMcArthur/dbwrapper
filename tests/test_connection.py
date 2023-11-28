import pytest

from dotenv import dotenv_values, find_dotenv
from db_wrapper.connection import DatabaseConnection

env_values = dotenv_values(find_dotenv())

class TestDatabaseConnection:

    @pytest.mark.parametrize("user, password, host, port, expected_result", [
        # Valid parameters
        (env_values['postgres_user'], env_values['postgres_pass'], 'localhost', 5432, True),
        # Invalid username
        ("invalid_user", env_values['postgres_pass'], 'localhost', 5432, False),
        # Invalid password
        (env_values['postgres_user'], "invalid_password", 'localhost', 5432, False),
        # Invalid username and password
        ("invalid_user", "invalid_password", 'localhost', 5432, False),
        # Invalid hostname
        (env_values['postgres_user'], env_values['postgres_pass'], 'bad.hostname.com', 5432, False),
        # Port non-integer
        (env_values['postgres_user'], env_values['postgres_pass'], 'bad.hostname.com', ';string', False),
        # Port negative
        (env_values['postgres_user'], env_values['postgres_pass'], 'bad.hostname.com', -5432, False),
        # Port too large
        (env_values['postgres_user'], env_values['postgres_pass'], 'bad.hostname.com', 115432, False)
    ])
    def test_username_password_validities(self, user, password, host, port, expected_result):
        test_database = 'postgres'

        try:
            connection = DatabaseConnection(host, port, user, password, test_database)
            if expected_result:
                assert connection is not None
            else:
                pytest.fail("Expected OperationalError, but no exception was raised")
        except (ValueError, ConnectionError):
            if expected_result:
                pytest.fail("Unexpected OperationalError when credentials are valid")