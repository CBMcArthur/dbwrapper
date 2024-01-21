import pytest

from dotenv import dotenv_values, find_dotenv
from db_wrapper import schema_management as sch
from db_wrapper.db_wrapper import DBWrapper

env_values = dotenv_values(find_dotenv())

class TestSchemaManagement:
    existing_schema = 'public'
    non_existing_schema = 'bad_schema'
    created_schema = 'test_schema'
    renamed_schema = 'test_renamed'

    def setup_class(cls):
        cls.wrapper = DBWrapper(host='localhost', port=5432, user=env_values['postgres_user'], password=env_values['postgres_pass'], database='postgres')

    def teardown_class(cls):
        pass

    def teardown_method(self, method):
        self.wrapper.execute_query(f"DROP SCHEMA IF EXISTS {self.created_schema} CASCADE")
        self.wrapper.execute_query(f"DROP SCHEMA IF EXISTS {self.renamed_schema} CASCADE")


    def test_existing_schema(self):
        result = sch.schema_exists(self.wrapper.get_db_connection(), self.existing_schema)
        assert result is True, f"Existing schema '{self.existing_schema}' should return True"

    def test_non_existing_schema(self):
        result = sch.schema_exists(self.wrapper.get_db_connection(), self.non_existing_schema)
        assert result is False, f"Non-existing schema '{self.non_existing_schema}' should return False"

    def test_create_schema(self):
        sch.create_schema(self.wrapper.get_db_connection(), self.created_schema)
        result = sch.schema_exists(self.wrapper.get_db_connection(), self.created_schema)
        assert result is True, f"Created schema '{self.created_schema}' should return True"

    @pytest.mark.parametrize("old_name, new_name, expected_result, error_msg_prefix", [
        (non_existing_schema, renamed_schema, False, "Error:"),
        (created_schema, existing_schema, False, "Error:"),
        (created_schema, renamed_schema, True, None)
    ])
    def test_rename_schema(self, old_name, new_name, expected_result, error_msg_prefix):
        sch.create_schema(self.wrapper.get_db_connection(), self.created_schema)
        result = sch.rename_schema(self.wrapper.get_db_connection(), old_name, new_name)
        if expected_result is False:
            assert result.startswith(error_msg_prefix), f"Renaming schema should return an error message"
        else:
            assert result == expected_result, f"Renaming schema should return {expected_result}"

    def test_delete_schema(self):
        sch.create_schema(self.wrapper.get_db_connection(), self.created_schema)
        result = sch.delete_schema(self.wrapper.get_db_connection(), self.created_schema)
        assert result == True, f"Deleting schema should return True"
        assert not sch.schema_exists(self.wrapper.get_db_connection(), self.created_schema), f"Schema should not exist after deletion"