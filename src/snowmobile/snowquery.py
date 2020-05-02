import snowflake.connector as sf
import pandas as pd
import json
from snowflake.connector import DictCursor
import os


class Snowflake:
    """Primary Connection and Query Execution Class"""

    def __init__(self, config_file: str = 'snowflake_config.json',
                 connection: str = '') -> None:
        """

        Args:
            config_file: Name of .json configuration file following the
            format of connection_credentials_SAMPLE.json.
            connection: Name of connection within json file to use - it will
            use first set of credentials in the file if no argument is passed.
        """
        self.config_file = config_file
        self.connection = connection

        for dirpath, dirnames, files in os.walk(os.path.expanduser('~'),
                                                topdown=False):
            if config_file in files:
                self.path_to_config = os.path.join(dirpath, config_file)
                break
            else:
                self.path_to_config = ''
                pass

        if not self.path_to_config:
            print(f"Could not find {config_file} in file system")
        else:
            print(f"Located configuration in file system at:\n\t"
                  f"{self.path_to_config}")
            with open(self.path_to_config) as c:
                configs = json.load(c)

        if not self.connection:
            self.connection = next(iter(configs.keys()))
        else:
            pass

        self.cfg = configs.get(self.connection)

        self.con = sf.connect(
            user=self.cfg["username"],
            password=self.cfg["password"],
            role=self.cfg["role"],
            account=self.cfg["account"],
            warehouse=self.cfg["warehouse"],
            database=self.cfg["database"],
            schema=self.cfg["schema"]
        )

    def execute_query(self, query: str, from_file: bool = False) \
            -> pd.DataFrame:
        """Run commands in Snowflake.

        Args:
            query: Raw SQL to execute.
            from_file: Boolean value indicating whether or not to read the
            query from a local file.
        """

        def read_query(filepath: str) -> object:
            """Fetch query text.

            Args:
                filepath:  Full file path to sql file - must be located
                in queries folder.

            query_text: (str) Snowflake query string.
            """

            try:
                query_text = open(filepath).read()
                return query_text
            except:
                query_text = None
                print("There was an error reading the file.")

            return query_text

        if from_file:
            try:
                query = read_query(query)
            except:
                print('Error reading query from file.')

        try:
            results = pd.read_sql(query, self.con)

        except sf.errors.ProgrammingError as e:

            results = None

            print(e)  # default error message

            print(f'Error {e.errno} ({e.sqlstate}): '
                  f'{e.msg} ({e.sfqid})')  # custom error message

        return results
