
import re
import sqlparse
from IPython.core.display import display, Markdown
from snowmobile import snowquery
import os


class Statement:

    def __init__(self, sql: str, connector: snowquery.Connector = ''):

        self.sql = sql
        self.connector = connector

    def render(self) -> object:
        """Renders SQL as markdown when called in IPython environment.

        Useful when combining explanation of different components of a SQL
        script in IPython or similar environment.

        """
        if isinstance(self.sql, list):
            self.sql = ';\n\n'.join(self.sql)

        display(Markdown(f"```mysql\n{self.sql}\n```"))

        return None

    def raw(self) -> object:
        """Returns raw SQL text."""

        return self.sql

    def execute(self, results: bool = True, render: bool = False,
                describe: bool = False) -> object:
        """Executes sql with option to return results / render sql as Markdown.

        Args:
            results: Boolean indicating whether or not to
            return results
            render: Boolean indicating whether or not to render the
            raw sql as markdown
            describe: Boolean indicating whether or not to
            print output of a pandas df.describe() on returned results (mostly
            useful for QA queries that are expected to return null-sets)
        """
        self.returned = self.connector.execute_query(self.sql)

        if render:
            self.render()

        if describe:
            print(f"\n-----results.describe()-----\n"
                  f" {self.returned.describe()}\n")

        if results:
            return self.returned


class Script(Statement):
    """
    Active-parsing of SQL script in Python for more seamless development loop.

    Enables flexible parsing of a SQL script into individual statements or
    spans of statements based on statement headers declared in SQL script.
    These should be denoted by placing headers above the statements within
    within the SQL file as shown below:

    .. code-block:: sql

        /*-statement_header-*/
        create or replace table sample_table as
        select
            *
        from sample_other

    Instantiate an instance of 'script' by calling Script class
    on a path to a SQL script.

    Structured this way so that each method within the class
    re-instantiates a new instance of 'script' (i.e. re-imports the raw
    file from local).

    This keeps users from having to alter or re-import anything on the
    Python side while editing & saving changes to the SQL file as well as
    ensures that all methods are using the latest version of the script.

    Args:
        path (str): Full path to SQL script including .sql extension
        pattern (str): Regex pattern that SQL statement headers are wrapped in
        connector (snowquery.Connector): Instantiated snowquery.Connector
        instance to use in the execution of Script or Statement objects

    Instantiated Attributes:
        script_txt (str): Raw SQL script read in as text file
        list_of_statements: (list) list of all SQL statements in SQL script
        (both with and without headers)
        statement_names (list): List of statement headers - including blank
        entries for statements that do not have headers (these instances
        have entries equal to '[]').
        statement (dict): Dictionary containing {'header': 'associated_sql'}.
        Note the conditionals within the comprehensions that throw out
        entries from both lists that do not have a valid statement_header
        spans (dict): Dictionary containing {'header':
        'index_position_in_script'} and is used in the get_span() method
        ordered_statements (list): List of statements in the order in which
        they appear in the script. Note that these are only statements that
        have a valid headers in the SQL file
        header_statements (str): List of all statements with their headers
        prepended to them, same length as ordered_statements
        full_sql (str): String containing all statements with valid headers
        combined into a single string.

    """

    def __init__(self, path: str, pattern: str = r"/\*-(\w.*)-\*/",
                 connector: snowquery.Connector = ''):
        super().__init__(self)
        self.source = path
        self.script_name = os.path.split(self.source)[-1]
        self.connector = connector
        # self.name = os.path.split(self.source)[-1].split('.sql')[0]
        self.pattern = re.compile(pattern)
        with open(self.source, 'r') as f:
            self.script_txt = f.read()
        self.statement_names = \
            [self.pattern.findall(self.statement) for
             self.statement in sqlparse.split(self.script_txt)]
        self.list_of_statements = \
            [sqlparse.format(val, strip_comments=True).strip()
             for val in sqlparse.split(self.script_txt)]
        self.stripped_statements \
            = [sqlparse.format(val, strip_comments=True).strip()
               for val in self.list_of_statements]
        self.statements = {
            k[0].lower(): v for k, v in zip(self.statement_names,
            self.stripped_statements) if k != [] and isinstance(v, str)
        }
        self.spans = {val: i for i, val in enumerate(self.statements.keys())}
        self.ordered_statements = [val for val in self.statements.values()]
        self.header_statements = [f"/*-{head.upper()}-*/\n{sql}"
                                  for head, sql in self.statements.items()]
        self.full_sql = ";\n\n".join(self.header_statements)
        self.returned = {}

    def reload_source(self) -> object:
        """Reloads script from source file path.

        """
        with open(self.source, 'r') as f:
            self.script_txt = f.read()

        return self

    def run(self, verbose=True) -> None:
        """Executes entire script a statement at a time."
        """
        self.reload_source()

        if not self.connector:
            self.connector = snowquery.Connector()

        for i, statement in enumerate(self.stripped_statements):

            if statement:
                self.connector.execute_query(statement, results=False)

                if verbose:
                    print(f"<finished executing {i} of "
                          f"{len(self.stripped_statements)}>")

        return None

    def get_statements(self) -> object:
        """Gets dictionary of unique Statement objects & associated methods.

        Returns:
            Dictionary of unique Statement objects & following methods:

        .. code-block:: python

            statement.render()
            statement.run()
            statement.execute()
        """
        self.reload_source()
        self.statements = Script(self.source).statements
        for k, v in self.statements.items():
            self.returned[k] = Statement(v, self.connector)

        return self.returned

    def fetch(self, header) -> object:
        """Fetches a single statement object out of the script.

        Most convenient method to execute individual statements - use
        get_statements() method if wanting to iterate over multiple
        statement objects and access the same methods

        Args:
            header: Header/label of the statement to fetch

        Returns:
            Statement object on which the following methods can be called:

        .. code-block:: python

            statement.render()
            statement.run()
            statement.execute()
        """
        return self.get_statements().get(header)

    def get_type(self, pattern: str = r'\[(.*)\]'):
        """Gets type of script 'Automation' or 'Rebuild' based on convention.

        Args:
            pattern: Pattern to identify script description, for example the
            name of 'Sample Script' should be 'Sample Script [
            Automation].sql' if it's an automation script

        Returns:
            Type of script (automation or rebuild)

        """
        type_brackets = self.script_name.split('.')[0].split(' ')[-1]

        if re.findall(pattern, type_brackets):
            return re.findall(pattern, type_brackets)[0].title()
        else:
            return ''
