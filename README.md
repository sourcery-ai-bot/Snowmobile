# snowmobile

`snowmobile` is a set of modules for streamlined querying, data-loading, and overall interaction with the Snowflake Database - a brief overview of each is outlined below.


<h3>snowquery</h3>
`snowquery` streamlines the authorization & execution of statements against the database via `execute_query()`, which:
- Establishes a connection to the Database, defaulting to remain on the same session unless force-disconnected via `snowquery.disconnect()`
- Defaults to return queried results in a [pandas](https://pandas.pydata.org/) DataFrame
- Includes parameter-based support for multiple sets of credentials stored in a local *snowflake_credentials.json* file
    - The locating & parsing of the credentials file is extracted into its own `snowcreds` sub-module for easier
        adaptation to security standards without needing to interact with the rest of `snowmobile`'s core functionality




<h3>snowloader</h3>
- `snowloader` streamlines the bulk-loading protocol outlined in the [Snowflake documentation](https://docs.snowflake.com/en/user-guide/data-load-overview.html)
    in the form of a `df_to_snowflake()` function, of which the primary features are:
    - Standardizing of DataFrame's columns prior to loading into the warehouse
    - DDL creation & execution if a pre-defined table to load data into doesn't exist 
    - Parameter-based flexibility to append DataFrame's contents or replace pre-existing contents
    

<h3>snowparser</h3>
- `snowparser` is a simple module for parsing statements and *spans* of statements
    from within _.sql_ files via the `get_statement()` function
    - Primary benefit is that it re-imports the _.sql_ file each the function is called, avoiding
    the need to re-instantiate or import an instance of `script` each time an edit is made to the _.sql_ file 
    
