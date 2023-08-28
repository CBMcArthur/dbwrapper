import os
import pytest
import dotenv

from db_wrapper.connection import DatabaseConnection

dotenv.load_dotenv(dotenv.find_dotenv())

class TestDatabaseConnection:
    @pytest.mark.parametrize("user, password, host, port, expected_result", [
        # Valid parameters
        (os.getenv('postgres_user'), os.getenv('postgres_pass'), 'localhost', 5432, True),
        # Invalid username
        ("invalid_user", os.getenv('postgres_pass'), 'localhost', 5432, False),
        # Invalid password
        (os.getenv('postgres_user'), "invalid_password", 'localhost', 5432, False),
        # Invalid username and password
        ("invalid_user", "invalid_password", 'localhost', 5432, False),
        # Invalid hostname
        (os.getenv('postgres_user'), os.getenv('postgres_pass'), 'bad.hostname.com', 5432, False),
        # Port non-integer
        (os.getenv('postgres_user'), os.getenv('postgres_pass'), 'bad.hostname.com', ';string', False),
        # Port negative
        (os.getenv('postgres_user'), os.getenv('postgres_pass'), 'bad.hostname.com', -5432, False),
        # Port too large
        (os.getenv('postgres_user'), os.getenv('postgres_pass'), 'bad.hostname.com', 115432, False)
    ])
    def test_username_password_validities(self, user, password, host, port, expected_result):
        test_database = 'postgres'

        try:
            dbconnection = DatabaseConnection(host, port, user, password, test_database)
            if expected_result:
                assert dbconnection is not None
            else:
                pytest.fail("Expected OperationalError, but no exception was raised")
        except (ValueError, ConnectionError):
            if expected_result:
                pytest.fail("Unexpected OperationalError when credentials are valid")