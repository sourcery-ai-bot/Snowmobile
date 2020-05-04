from snowmobile import snowconn, snowloader, snowquery
from snowflake.connector import DictCursor


# from snowmobile import snowquery

conn = snowconn.Connection(conn_name='CLC').get_conn()
conn.config_file
type(conn)
# conn = Connection().get_conn()
cursor = conn.cursor(DictCursor)
# cursor.execute('SELECT * FROM ALIGN_DIM_V1 LIMIT 10')
# cursor.execute('CREATE OR REPLACE TABLE GEM_JUNK_ONE AS SELECT 1 AS DUMMY')
# cursor.fetch_pandas_all()
# cursor.fetchall()

from importlib import reload
reload(snowquery)
reload(snowloader)

from snowmobile import snowquery
from snowmobile import snowloader

sf = snowquery.Snowflake()

df = sf.execute_query('SELECT * FROM ALIGN_DIM_V1 LIMIT 10')
df2 = sf.execute_query('SELECT * FROM ALIGN_DIM_V1 LIMIT 20')

test = snowloader.df_to_snowflake(df, 'GEM_TEST_SNOWLOADER_JUNK', snowflake=sf)
print(test)

sf.execute_query('DROP TABLE GEM_TEST_SNOWLOADER_JUNK')

snowquery.Snowflake()

snowquery.execute_query('SELECT * FROM ALIGN_DIM_V1 LIMIT 10')

clc = snowquery.Snowflake(conn_name='CLC')
clc.conn_name

