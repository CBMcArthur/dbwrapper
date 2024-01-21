
class DatabaseErrorException(Exception):
    def __init__(self, message, sql_query=None):
        super().__init__(message)
        self.message = message
        self.sql_query = sql_query

    def __err__(self):
        return f"{self.message}\SQL Query: {self.sql_query}"


class DatabaseWarningException(Exception):
    def __init__(self, message, sql_query=None):
        super().__init__(message)
        self.message = message
        self.sql_query = sql_query

    def __err__(self):
        return f"{self.message}\SQL Query: {self.sql_query}"