

import snowflake.connector
from snowmobile import snowcreds as creds
from fcache.cache import FileCache

cache = FileCache('snowmobile', flag='cs')

class Connection(creds.Credentials):
    """Instantiate with inherited attributes from snowcreds.

    Args:
        config_file: Name of .json configuration file following the
            format of connection_credentials_SAMPLE.json.
        conn_name: Name of connection within json file to use - it will
            use first set of credentials in the file if no argument is
            passed.

    """
    def __init__(self, config_file: str = 'snowflake_credentials.json',
                 conn_name: str = '', cache=cache):
        super().__init__()

        self.config_file = config_file
        self.conn_name = conn_name
        self.cache = cache

    def get_conn(self) -> snowflake.connector:
        """Uses credentials to authenticate for statement execution.

        Returns:
            snowflake.connector.conn object

        """

        self.credentials = creds.Credentials(config_file=self.config_file,
                                             conn_name=self.conn_name,
                                             cache=self.cache).get()
        self.conn = snowflake.connector.connect(
            user=self.credentials["username"],
            password=self.credentials["password"],
            role=self.credentials["role"],
            account=self.credentials["account"],
            warehouse=self.credentials["warehouse"],
            database=self.credentials["database"],
            schema=self.credentials["schema"])

        return self.conn
