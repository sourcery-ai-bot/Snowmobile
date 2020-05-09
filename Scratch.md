


```bash
In[1]: dsc = snowquery.Snowflake(conn_name='DSC')
Searching for snowflake_credentials.json in local file system..
Located & loaded snowflake_credentials.json from:
	C:\Users\GEM7318\Documents\Github\snowflake\snowflake_credentials.json
```

## Implicit / default connection credentials

`In[1]:`
```python
    dsc = snowquery.Snowflake(conn_name='DSC')
```

`Out[1]:`
```console
Locating & importing credentials..
	<1 of 4> Searching for snowflake_credentials.json in local file system..

	<2 of 4> Located & loaded snowflake_credentials.json from:
		C:\Users\GEM7318\Documents\Github\snowflake\snowflake_credentials.json

	<3 of 4> No explicit connection passed, fetching 'DSC' credentials by default

	<4 of 4> Successfully imported credentials for conn_name='DSC'
```

## Explicit / specifying set of credentials

`In[2]:`
```python
    dsc = snowquery.Snowflake(conn_name='DSC')
```

`Out[2]:`
```console
Locating & importing credentials..
	<1 of 4> Searching for snowflake_credentials.json in local file system..

	<2 of 4> Located & loaded snowflake_credentials.json from:
		C:\Users\GEM7318\Documents\Github\snowflake\snowflake_credentials.json

	<3 of 4> Fetching 'CLC' credentials from snowflake_credentials.json

	<4 of 4> Successfully imported credentials for conn_name='CLC'
```