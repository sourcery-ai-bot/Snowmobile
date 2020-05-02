## Project Description

##snowmobile

`snowmobile` is a set of modules for streamlined querying, data-loading, and overall interaction with the Snowflake Database


Modules are each built to address a common action, with the primary focus being the following:<br>
- `snowquery` provides easy authorization execution of statements against the database with support for 
    the `execute_query()` function, defaulting to return queried results in a [pandas](https://pandas.pydata.org/)
    DataFrame and includes parameter-based support for multiple sets of credentials stored in a local *snowflake_credentials.json* 
    file to enable users who interact with different warehouses or schemas on an ongoing basis.
    - The locating & parsing of the credentials file is extracted into its own `snowcreds` sub-module for easier
    adaptation to security standards without needing to interact with the rest of `snowmobile`'s core functionality 
    
    
    
- `snowloader` streamlines the bulk-loading protocol outlined in the [Snowflake documentation](https://docs.snowflake.com/en/user-guide/data-load-overview.html)
    in the form of a `df_to_snowflake()` function, of which the primary features are:
    - Standardizing of DataFrame's columns prior to loading into the warehouse
    - DDL creation & execution if a pre-defined table to load data into doesn't exist 
    - Parameter-based flexibility to append DataFrame's contents or replace pre-existing contents
    
    
- `snowparser` is a simple module for parsing statements and *spans* of statements
    from within _.sql_ files via the `get_statement()` function that re-imports the _.sql_ file
    each time it is called - the primary benefit of this is it avoidings the need to re-instantiate
    or import an instance of `script` each time an edit is made to the _.sql_ file 
    


