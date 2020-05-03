import snowflake.connector as sf
import pandas as pd
from snowmobile import snowconn


class Snowflake(snowconn.Connection):
    """Primary Connection and Query Execution Class"""

    def __init__(self, config_file: str = 'snowflake_config.json',
                 conn_name: str = '') -> None:
        super().__init__(config_file, conn_name)

        self.conn = snowconn.Connection(config_file=self.config_file,
                                        conn_name=self.conn_name).get_conn()

    def execute_query(self, query: str, from_file: bool = False,
                      filepath='') -> pd.DataFrame:
        # con = self.conn
        """Run commands in Snowflake.

        Args:
            query: Raw SQL to execute.
            from_file: Boolean value indicating whether or not to read the
            query from a local file.
        """
        if from_file and filepath:

            try:
                query = open(filepath).read()

            except sf.errors.ProgrammingError as e:
                print("There was an error reading the file.")
                query = None

        if query:

            try:
                results = pd.read_sql(query, self.conn)

            except sf.errors.ProgrammingError as e:
                print(e)  # default error message
                print(f'Error {e.errno} ({e.sqlstate}): {e.msg} ('
                      f'{e.sfqid})')  # custom error message
                results = None

        else:
            results = None

        return results

    def disconnect(self):
        self.conn.close()
        return None