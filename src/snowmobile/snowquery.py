import snowflake.connector as sf
import pandas as pd
import json
from snowflake.connector import DictCursor
import os


class Snowflake:
    """Primary Connection and Query Execution Class"""

    def __init__(self, config_file='snowflake_config.json', connection=''):

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

    def execute_query(self, query, from_file=False):
        """Run commands in Snowflake.
        """

        def read_query(filepath):
            """Fetch query text.
            Input:
            -------------------------
            file_name:  (str) File name, including suffix. Must be
                located in queries folder.
            Output:
            -------------------------
            query_text: (str) Snowflake query string.
            """

            try:
                query_text = open(filepath).read()
                return query_text
            except:
                print("There was an error reading the file.")
                return None

        if from_file:
            try:
                query = read_query(query)
            except:
                print('Error reading query from file.')

        try:
            results = pd.read_sql(query, self.con)
            return results

        except sf.errors.ProgrammingError as e:
            # default error message
            print(e)
            # customer error message
            print(f'Error {e.errno} ({e.sqlstate}): {e.msg} ({e.sfqid})')

        return None
