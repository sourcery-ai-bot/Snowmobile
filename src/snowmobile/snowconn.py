
# Imports
import snowflake.connector as sf
from snowmobile import snowcreds as creds


class Connection(creds.Credentials):

    def __init__(self, config_file: str = 'snowflake_config.json',
                 conn_name: str = ''):
        super().__init__()

        self.config_file = config_file
        self.conn_name = conn_name

    def get_conn(self):

        self.credentials = creds.Credentials(config_file=self.config_file,
                                             conn_name=self.conn_name).get()
        self.conn = sf.connect(
            user=self.credentials["username"],
            password=self.credentials["password"],
            role=self.credentials["role"],
            account=self.credentials["account"],
            warehouse=self.credentials["warehouse"],
            database=self.credentials["database"],
            schema=self.credentials["schema"]
        )

        return self.conn

# conn = Connection().get_conn()
# cursor = conn.cursor(DictCursor)
# cursor.execute('SELECT * FROM ALIGN_DIM_V1 LIMIT 10')
# cursor.execute('CREATE OR REPLACE TABLE GEM_JUNK_ONE AS SELECT 1 AS DUMMY')
# cursor.fetch_pandas_all()
# cursor.fetchall()
