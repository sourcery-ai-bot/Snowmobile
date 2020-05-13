
import snowflake.connector as sf
import pandas as pd
from snowmobile import snowconn


class Connector(snowconn.Connection):
    """Primary Connection and Query Execution Class"""

    def __init__(self, config_file: str = 'snowflake_credentials.json',
                 conn_name: str = '') -> None:
        super().__init__(config_file, conn_name)

        self.conn = snowconn.Connection(config_file=self.config_file,
                                        conn_name=self.conn_name).get_conn()

    def execute_query(self, query: str, from_file: bool = False,
                      filepath: str = '', results: bool = True) -> \
            pd.DataFrame:
        """Run commands in Snowflake.

        Args:
            query: Raw SQL to execute.
            from_file: Boolean value indicating whether or not to read the
            query from a local file.
            filepath: Full file path to .sql script if looking to execute a
            single-statement script on the fly, most useful for executing
            extracted DDL as part of a cleanup.
            results: Boolean value indicating whether or not to return results
        Returns:
            Results from query in a Pandas DataFrame by default or None if
            ``return_results=False`` is passed when function is called.
        """
        self.query = query
        self.from_file = from_file
        self.filepath = filepath
        self.results = results

        if self.from_file and self.filepath:

            try:
                self.query = open(self.filepath).read()

            except sf.errors.ProgrammingError as e:
                print("There was an error reading the file.")
                self.query = None

        if self.query:

            try:
                self.results = pd.read_sql(query, self.conn)

            except sf.errors.ProgrammingError as e:
                print(e)  # default error message
                print(f'Error {e.errno} ({e.sqlstate}): {e.msg} ('
                      f'{e.sfqid})')  # custom error message
                self.results = None

        else:
            self.results = None

        if self.results:
            return self.results

        else:
            return None

    def disconnect(self) -> None:
        """Disconnect from connection with which Connect() was instantiated.

        Returns:
            None

        """
        self.conn.close()
        return None
