import pytest
import sqlparse

from sqlalchemy import text
from unittest.mock import Mock, patch
from db_wrapper.connection import DatabaseConnection
from db_wrapper import query_execution


class TestQueryExecution:

    parameterized_query_inputs = [
        ("SELECT * FROM tablea", "SELECT", "SELECT * FROM tablea", None),
        ("update tablea", "UPDATE", "UPDATE tablea", None),
        ("DROP tablea", "DROP", "DROP tablea", None),
        ("Bad SQL", None, None, ValueError),
        ({'col': 'value'}, None, None, ValueError)
    ]

    @pytest.mark.parametrize("sql, expected_type, formatted_sql, expected_exception", parameterized_query_inputs)
    def test_validate_sql(self, sql, expected_type, formatted_sql, expected_exception):
        if expected_exception is not None:
            with pytest.raises(expected_exception):
                query_execution.validate_sql(sql)
        else:
            result = query_execution.validate_sql(sql)
            assert len(result) == 1
            assert isinstance(result, tuple)
            assert all(isinstance(stmt, sqlparse.sql.Statement) for stmt in result)

            result = query_execution.validate_sql(text(sql))
            assert len(result) == 1
            assert isinstance(result, tuple)
            assert all(isinstance(stmt, sqlparse.sql.Statement) for stmt in result)

    @pytest.mark.parametrize("sql, expected_type, formatted_sql, expected_exception", parameterized_query_inputs)
    def test_get_query_type(self, sql, expected_type, formatted_sql, expected_exception):
        if expected_exception is not None:
            with pytest.raises(expected_exception):
                query_execution.get_query_type("Wrong type")
        else:
            assert query_execution.get_query_type(sql) == expected_type

    # test_format_sql
    @pytest.mark.parametrize("sql, expected_type, formatted_sql, expected_exception", parameterized_query_inputs)
    def test_format_sql(self, sql, expected_type, formatted_sql, expected_exception):
        if expected_exception is not None:
            with pytest.raises(expected_exception):
                query_execution.format_sql(sql)
        else:
            assert query_execution.format_sql(sql) == formatted_sql


    def test_prep_results_select(self):
        query_type = "SELECT"
        results = [{"column1": "value1", "column2": "value2"}, {"column1": "value1", "column2": "value2"}, {"column1": "value1", "column2": "value2"}]
        mock_results = Mock()
        mock_results.mappings.return_value = results

        formatted_results = query_execution.prep_results(mock_results, query_type)
        assert formatted_results == results
        mock_results.mappings.assert_called_once()

    def test_prep_results_rowcount(self):
        query_types = ["INSERT", "UPDATE", "DELETE"]
        results = [{"column1": "value1", "column2": "value2"}, {"column1": "value1", "column2": "value2"}, {"column1": "value1", "column2": "value2"}]
        mock_results = Mock()
        mock_results.rowcount.return_value = len(results)

        for type in query_types:
            formatted_results = query_execution.prep_results(mock_results, type)
            assert formatted_results == 3

    def test_prep_results_bool(self):
        query_types = ["CREATE", "DROP", "ALTER", "DO"]
        for type in query_types:
            formatted_results = query_execution.prep_results(None, type)
            assert formatted_results is True


    @patch('db_wrapper.connection.DatabaseConnection')
    def test_execute_statement(self, mock_databaseconnection):
        sql = 'SELECT * FROM tablea'
        expected_results = [{"column1": "value1", "column2": "value2"}, {"column1": "value1", "column2": "value2"}, {"column1": "value1", "column2": "value2"}]

        mock_results = Mock()
        mock_results.rowcount.return_value = len(expected_results)
        mock_results.mappings.return_value = expected_results

        mock_conn = mock_databaseconnection.get_db_engine().connect().__enter__.return_value
        mock_conn.execute.return_value = mock_results

        results = query_execution.execute_query(mock_databaseconnection, sql)

        assert results == expected_results
        mock_conn.commit.assert_called_once()
