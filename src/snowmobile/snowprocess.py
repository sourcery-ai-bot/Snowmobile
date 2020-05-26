
from snowmobile import snowscripter
import os
import collections


class SQL:
    """A simple class for executing SQL files store in a single directory.

    Args:
        path_to_sql: Path to a directory containing SQL scripts following
        the format of #_name_of_script.sql where '#' is the execution order
        number (including directories storing a single script)

    Attributes:
        sql: Dictionary of script name to full path to script
        sql_script_no: Dictionary of file execution number to file name
        sql_ordered_keys: Sorted list of of execution number keys
        sql_sorted: Ordered dict of script name to snowscripter.Script object
    """

    def __init__(self, path_to_sql: str):
        self.path_to_sql = path_to_sql

        self.sql: dict = \
            {file: os.path.join(self.path_to_sql, file)
             for file in os.listdir(self.path_to_sql)
             if os.path.splitext(file)[1] == r'.sql'}

        self.sql_script_no: dict = \
            {os.path.basename(file.split('_')[0]): file
             for file in self.sql.keys()}

        self.sql_ordered_keys: list = \
            sorted(list(self.sql_script_no.keys()))

        self.sql_sorted: collections.OrderedDict = \
            collections.OrderedDict(
                {k: snowscripter.Script(self.sql
                                        .get(self.sql_script_no.get(k)))
                 for k in self.sql_ordered_keys})

    def run(self) -> None:
        """Executes every sql script in the correct order"

        Returns:
            None
        """
        for _, script in self.sql_sorted.items():
            script.run()

        return None


class Process:
    """Class for 'Process' directory w/ Automation & Rebuild child
    directories of a _sql folder.

    Args:
        path_to_process: Path to project/process directory

    Attributes:
        path_to_automation: Path to the automation sql directory
        path_to_rebuild: Path to the rebuild sql directory
        automation: A SQL object from the automation sql directory
        rebuild: A SQL object from the rebuild sql directory

    """

    def __init__(self, path_to_process: str):
        self.path_to_process: str = path_to_process

        self.path_to_automation: str = \
            os.path.join(path_to_process, '_sql', 'Automation')

        self.path_to_rebuild: str = \
            os.path.join(path_to_process, '_sql', 'Rebuild')

        if os.path.isdir(self.path_to_automation):
            self.automation: SQL = SQL(self.path_to_automation)

        if os.path.isdir(self.path_to_rebuild):
            self.rebuild: SQL = SQL(self.path_to_rebuild)
