[![PyPI version](https://badge.fury.io/py/snowmobile.svg)](https://badge.fury.io/py/snowmobile)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://github.com/GEM7318/Snowmobile/blob/master/LICENSE.txt)
[![Documentation Status](https://readthedocs.org/projects/snowmobile/badge/?version=latest)](https://snowmobile.readthedocs.io/en/latest/?badge=latest)

# snowmobile

`snowmobile` is a simple set of modules for streamlined interaction with the Snowflake Database for Data Scientists and Business Analysts.

As such the included codes are intended to be used for the execution of raw SQL and don't make use any ORM to map Python objects to tabular Snowflake
counterparts.
 
A quick overview of simplified usage and a description of of each module is outlined below.

---
## Basic usage
1. Install with `pip install snowmobile`

2. Create file called *snowflake_credentials.json* following the below structure with as many sets of credentials
as desired and store anywhere on local file system
    ```json
    {
        "Connection1": {
        "username":	"",
        "password":	"",
        "role": "",
        "account": "",
        "warehouse": "warehouse #1",
        "database":	"database #1",
        "schema": "schema #1"
      },
        "SANDBOX": {
        "username":	"",
        "password":	"",
        "role": "",
        "account": "",
        "warehouse": "warehouse #1",
        "database":	"database #1",
        "schema": "SANDBOX"
      }
    }
   ```
        
3. Import desired modules and execute a statement to test connection
    ```python
    # bundled authentication & statement-execution module  
    from snowmobile import snowquery
   
    # Instantiate an instance of a connection
    sf = snowquery.Snowflake(conn_name='SANDBOX')
   
    # Execute statements on that connection 
    sample_table = sf.execute_query('SELECT * FROM SAMPLE_TABLE')
    ```


---
## snowcreds
`snowcreds` is comprised of a single class intentionally isolated for easier evolving along with security standards, 
its instantiation of `Credentials()` accepts the below two arguments and associated defaults
```python
def __init__(self, config_file: str = 'snowflake_credentials.json',
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
## snowconn
`snowconn` is also comprised of a single class, `Connection()`, that inherits `Credentials()` to retrieve a set of credentials with which to establish a connection to the database.

Once instantiated with the inherited `config_file` and `conn_name` attributes, the `.get_conn()` method will locate the credentials file, import the specified credentials, authenticate and return a `conn` 
object as shown below for an illustrative **SANDBOX** set of credentials within the config file.
```python
from snowmobile import snowconn
creds = snowconn.Connection(conn_name='SANDBOX').get_conn()
```

---
## snowquery
`snowquery` makes use of the two modules above for simplified execution of statements against the database via an `execute_query()` method, which, itself
uses [pandas'](https://pandas.pydata.org/) `pd.read_sql` function to execute the SQL and return results from the DataBase
as a [dataframe](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html) by default.

Its usage to query via set of credentials stored in _snowflake_credentials.json_ labeled **SANDBOX** is as follows.

```python
from snowmobile import snowquery
sf = snowquery.Snowflake(conn_name='SANDBOX')
sample_table = sf.execute_query('SELECT * FROM SAMPLE_TABLE')
```


---
## snowloader
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
# ... continuation from snowquery example above

from snowmobile import snowloader
floated_sample = sample_table.applymap(float)

# Option 1 
snowloader.df_to_snowflake(df=floated_sample, table_name='SAMPLE_TABLE', force_recreate=True,
                            snowflake=sf)

# Option 2 
snowloader.df_to_snowflake(df=floated_sample, table_name='SAMPLE_TABLE', force_recreate=True)
``` 

In the above,
- *Option 1* will load the data back into Snowflake on the same connection that was established in the
  `sf = snowquery.Snowflake(conn_name='SANDBOX')` statement by use of the `snowflake=sf` parameter
- In *Option 2* this argument is omitted and the function will instantiate a new connection based on the first set of credentials in **snowflake_credentials.json**


In general and particularly when iteratively loaded multiple files into the database, it will be faster to instantiate a single instance of `snowquery`
that's passed into the `df_to_snowflake()` function so that it does not need to find, read-in and parse the credentials file each time its called.

---
## scriptparser
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
  
