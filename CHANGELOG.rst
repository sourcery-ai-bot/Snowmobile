
* Version 0.1.0
    - Stable version for Python 3.7 and 3.8
    - Addition of sqlparse and IPython to requirements.txt
    - Addition of ``snowscripter.render()`` for rendering sql in IPython/Jupyter environments
* Version 0.1.1
    - Simplifying ``snowscripter.raw()``
* Version 0.1.2
    - ``snowquery``
        - Change from ``snowquery.Snowflake()`` to ``snowquery.Connector()`` for semantic purposes / clarity of instantiation
    - ``snowscripter``
        - Addition of ``snowscripter.Script`` methods:
            - ``.reload_source()`` which is called in methods behind the scenes
            - ``.get_statements()`` to return iterable list of Statement objects
            - ``.fetch()`` to return a single Statement object from within the Script
        - Addition of ``snowscripter.Statement`` methods
            - ``.execute()`` to execute statement
                - Includes arguments ``return_results``, ``render``, and ``describe`` to avoid the need to call multiple methods if desire
                  is to combine the execution of a statement and rendering the sql code itself
            - ``.render()`` to render syntax-highlighted statement in IPython/Jupyter environments
            - ``.raw()`` to return the raw string of sql
* Version 0.1.3
    - Quick patch of HTML tag causing explosion in the docs
* Version 0.1.4
    - Switching dynamic tags to include beta indicator
* Version 0.1.5
    - Docs addition only
* Version 0.1.6
    - ``snowscripter``
        - Adding logic to strip comments from sql statements stored in the 'list_of_statements' attribute
          as trailing comments were causing errors in some instances of ``script.run()``
        - Adding 'verbose' argument to ``script.run()`` to print out a confirmation of each statement as it
          finishes executing
    - ``snowcreds``
        - Adding in a caching layer to first check a cache when looking for the path to
          the credentials and only traversing a file system if it comes up with nothing
* Version 0.1.7
    - Only change is fixing dependency issue with fcache
