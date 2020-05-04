# snowmobile

`snowmobile` is a simple set of modules for streamlined interaction with the Snowflake Database for Data Scientists and Business Analysts
who desire a more fluid workflow between raw SQL and Python. 

As such the included codes are intended to be used for the execution of raw SQL and don't make use any ORM to map Python objects to tabular Snowflake
counterparts.
 
A brief overview of each module is outlined below.


---
### snowcreds
`snowcreds` is comprised of a single class intentionally isolated for easier evolving along with security standards, 
its instantiation of `Credentials()` accepts the below two arguments and associated defaults
```python
def __init__(self, config_file: str = 'snowflake_config.json',
                 conn_name: str = '') -> None:
        """
        Instantiates an instance of credentials file
        
        Args:
            config_file: Name of .json configuration file following the
            format of connection_credentials_SAMPLE.json.
            conn_name: Name of connection within json file to use, will
            use first set of credentials in the file if no argument is passed.
        """
        self.config_file = config_file
        self.conn_name = conn_name
```
Once instantiated, the `.get()` method will traverse a user's file system and return the full path to the first file it finds with a filename matching `config_file`.

**The .json file itself is assumed to store its credentials following [this](https://github.com/GEM7318/Snowmobile/blob/master/connection_credentials_SAMPLE.json) format**

---
### snowconn
`snowconn` is also comprised of a single class, `Connection()`, that inherits `Credentials()` to retrieve a set of credentials with which to establish a connection to the database.

Once instantiated with the inherited `config_file` and `conn_name` attributes, the `.get_conn()` method will locate the credentials file, import the specified credentials, authenticate and return a `conn` 
object as shown below for an illustrative **SANDBOX** set of credentials within the config file.
```python
from snowmobile import snowconn
creds = snowconn.Connection(conn_name='SANDBOX').get_conn()
```

---
### snowquery
`snowquery` makes use of the two modules above for simplified execution of statements against the database via an `execute_query()` method, which, itself
uses [pandas'](https://pandas.pydata.org/) `pd.read_sql` function in conjunction to execute the SQL and will return results from the DataBase
in the form of a [dataframe](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html) by default.

Its usage to query via set of credentials stored in _snowflake_credentials.json_ labeled **SANDBOX** is as follows.

```python
from snowmobile import snowquery
sf = snowquery.Snowflake(conn_name='SANDBOX')
sample_table = sf.execute_query('SELECT * FROM SAMPLE_TABLE')
```


---
### snowloader
`snowloader` streamlines the bulk-loading protocol outlined in the [Snowflake documentation](https://docs.snowflake.com/en/user-guide/data-load-overview.html)
in the form of a `df_to_snowflake()` function and is intended to be a one-stop solution for the quick loading of data.

Its main features are:
- Standardizing of DataFrame's columns prior to loading into the warehouse
- DDL creation & execution if a pre-defined table to load data into doesn't exist 
- Parameter-based flexibility to append DataFrame's contents or replace pre-existing contents
- Returns a boolean indicating whether or not a load was successful for exception-handling when iteratively loading/appending multiple files
into a single table.

Continuing on the example above, the below will convert all columns in the _sample_table_ DataFrame to floats and re-load it into the warehouse,
executing new-DDL to create the table with float data types and loading all data back into the table.

```python
from snowmobile import snowquery
sf = snowquery.Snowflake(conn_name='SANDBOX')
sample_table = sf.execute_query('SELECT * FROM SAMPLE_TABLE')

from snowmobile import snowloader
floated_sample = sample_table.applymap(float)
snowloader.df_to_snowflake(df=floated_sample, table_name='SAMPLE_TABLE',
                             force_recreate=True)
``` 
  

---
### scriptparser
`scriptparser` is a simple module for parsing statements and *spans* of statements from within _.sql_ files via the `get_statement()` method.

`scriptparser` assumes headers have been added above each statement following the regex pattern:
- /\*-statement_header-\*/

Usage in its simplest form is as follows:
```python
from snowmobile import scriptparser as sp
script = sp.ParseScript(full_path_to_script.sql)
script.get_statement('header_of_desired_statement')
```

Its primary benefit is that it re-imports the _.sql_ file each time the `get_statement()` method is called, avoiding
the need to re-instantiate or import an instance of `script` each time an edit is made to the _.sql_ file 
  
