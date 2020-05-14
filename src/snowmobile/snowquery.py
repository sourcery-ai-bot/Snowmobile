
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

    def execute_query(self, query: str, results: bool = True) -> \
            pd.DataFrame:
        """Execute commands & query data from the warehouse.

        Args:
            query: Raw SQL to execute.
            results: Boolean value indicating whether or not to return results
        Returns:
            Results from query in a Pandas DataFrame by default or None if
            ``results=False`` is passed when function is called.
        """
        self.query = query
        self.return_results = results

        if self.query:

            try:
                self.results = pd.read_sql(query, self.conn)

            except sf.errors.ProgrammingError as e:

                print(e)  # default error message

                print(f'Error {e.errno} ({e.sqlstate}): {e.msg} ('
                      f'{e.sfqid})')  # custom error message

                self.results = pd.DataFrame()

        if self.return_results:
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

    def commit(self) -> None:
        """Manually commits changes to database in instances when needed.

        Returns:
            None
        """
        self.conn.commit()

        return None

    def new(self) -> object:
        """Instantiates a new session for the same object.

        Returns:
            A snowquery.Connector() under the same set of credentials as the
            originally instantiated object but connected to a new session.
        """
        self.conn = snowconn.Connection(config_file=self.config_file,
                                        conn_name=self.conn_name).get_conn()

        return self
