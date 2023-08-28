from sqlalchemy import URL, create_engine, text
from sqlalchemy.exc import OperationalError
from libraries import validation

class DatabaseConnection:
    def __init__(self, host, port, user, password, database):
        # Validate the parameters for the DB Connection
        if not all ([host, port, user, password, database]):
            raise ValueError("Connection Creation Error: one or more parameters "
                             "(host, post, user, password, and/or database) were not provided.")
        if not validation.is_valid_port(port):
            raise ValueError("An invalid port number was provided")
        if not validation.is_valid_hostname(host):
            raise ValueError("An invalid hostname was provided")

        # Create the URl for the connection string
        self.db_url = URL.create('postgresql',
            username=user,
            password=password,
            host=host,
            port=port,
            database=database
        )
        try:
            # Create DB engine object, then attempt to connect
            self.db_engine = create_engine(self.db_url)
            with self.db_engine.connect():
                pass
        except OperationalError as e:
            raise ConnectionError(f"Failed to create the database engine: {e}")

    def execute_query(self, sql):
        with self.db_engine.connect() as conn:
            results = conn.execute(text(sql))
        for row in results:
            print(row)
