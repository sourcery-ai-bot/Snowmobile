# Imports
from snowmobile import snowquery
import pandas as pd
import string
import os
import itertools
import csv
import datetime


def standardize_col(col: str) -> str:
    """Standardize a column for Snowflake table.
    (1) Replaces spaces with underscores, trims leading & trailing
        underscores, forces upper-case
    (2) Replaces special characters with underscores
    (3) Reduces repeated special characters

    Args:
        col: A single string value of a column name
    Returns:
        col: A string that has been re-formatted/standardized for Snowflake
                standards
    """
    col = ((col.replace(' ', '_')).strip('_')).upper()  # 1

    invalid_punct = [punct for punct in string.punctuation if
                     punct != '_']  # 2
    repl_invalid_punct = str.maketrans(''.join(invalid_punct),
                                       '_' * len(invalid_punct))
    col = ''.join(col.translate(repl_invalid_punct))

    new_chars = []  # 3

    for k, v in itertools.groupby(col):
        v_list = list(v)

        if k in string.punctuation or k in string.whitespace:
            new_chars.append(k)

        else:
            new_chars.append(''.join(v_list))

    col = ''.join(new_chars)

    return col


def rename_cols_for_snowflake(df: pd.DataFrame) -> pd.DataFrame:
    """Renaming DataFrame columns for Snowflake table.

    Args:
        df: pd.DataFrame to be pushed to Snowflake
    Returns:
        df: pd.DataFrame with re-formatted column names and a
            'loaded_tmstmp' field added on the far right side
    """
    df['LOADED_TMSTMP'] = datetime.datetime.now()
    old_cols = list(df.columns)
    new_cols = [standardize_col(val) for val in df.columns]
    new_col_map = {k: v for k, v in zip(old_cols, new_cols)}
    df = df.rename(columns=new_col_map)
    return df


def get_ddl(df: pd.DataFrame, table_name: str) -> str:
    """Converting mysql DDL to Snowflake DDL.

    Args:
        df: pd.DataFrame to push to Snowflake.
        table_name: Name of table to load the DataFrame into.
    Returns:
        str: DDL to be executed to create table structure to load DataFrame
            into (for force-recreation of a table or loading into a table that
            doesn't previously exist)
    """
    final_ddl = \
        pd.io.sql.get_schema(df, table_name).replace('CREATE TABLE',
                                                     'CREATE OR REPLACE TABLE')
    return final_ddl


def check_information_schema(table_name: str,
                             snowflake: snowquery.Snowflake) -> list:
    """Checks information schema for existence of table & returns columns
    for comparison to local DataFrame if so

    Args:
        table_name: Name of table to load the df into
        snowflake: snowquery.Snowflake object to execute statement with
    Returns:
        table_cols: Columns of the table within database or an empty list if
            not
    """

    sql = f"""SELECT
                ORDINAL_POSITION
                ,COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'
            ORDER BY 1 ASC"""

    validation_df = snowflake.execute_query(sql)

    try:
        table_cols = list(validation_df['COLUMN_NAME'])

    except:
        table_cols = []

    return table_cols


def compare_fields(df_cols: list, table_cols: list) -> int:
    """Returns match-count of column names in DataFrame compared to the table

    Args:
        df_cols: Columns of the DataFrame to load
        table_cols: Columns of the table the DataFrame is being loaded into
    Returns:
        matched_cnt: Count of matches between the table and the DataFrame's
            columns - will return a zero if the table does not exist at all
    """
    matched_list = []
    for i, (df_col, tbl_col) in enumerate(zip(df_cols, table_cols), start=1):

        if df_col == tbl_col:
            matched_list.append(i)

        else:
            pass

    matched_cnt = len(matched_list)

    return matched_cnt


def validate_table(df: pd.DataFrame, table_name: str,
                   snowflake: snowquery.Snowflake) -> tuple:
    """Analyzes count of matching columns to count of cols in df to load.

    Args:
        df: pd.DataFrame to load into Snowflake
        table_name: name of table (str) to load df into
    Returns:
        outcome: Tuple of boolean values indicating all possible
            combinations of a table existing (Y/N) and the columns of the
            table matching those in the DataFrame
    """
    table_cols = check_information_schema(table_name, snowflake)

    df_cols = list(df.columns)

    cnt_matches = compare_fields(df_cols, table_cols)

    if cnt_matches == len(df_cols) and cnt_matches == len(table_cols):
        outcome = True, True  # Table exists & fields match

    elif len(table_cols) != 0:
        outcome = True, False  # Table exists but fields don't match

    else:
        outcome = False, False  # Table doesn't exist

    return outcome


def verify_load(snowflake: snowquery.Snowflake,
                df: pd.DataFrame, table_name: str, force_recreate: bool =
                False) -> bool:
    """Performs pre-loading operations and checks to table in-warehouse
    # (1) Performs comparison of local DataFrame to in-warehouse table and
            creates/recreate the table if needed
    # (2) Will not automatically recreate a table if it already exists but
    #       cannot append the contents of the DataFrame without modification

    Args:
        df: pd.DataFrame to push to Snowflake
        table_name: string representation of table name to load the df into
        force_recreate: boolean value to indicating whether or not to
            force-recreation of table
    Returns:
        continue_load: Boolean value indicating whether or not the
            loading process will occur successfully if continued based on the
            local to in-warehouse comparison
    """
    print(f"<validating load into {table_name}>")

    df = rename_cols_for_snowflake(df)

    table_exists, fields_match = validate_table(df, table_name,
                                                snowflake=snowflake)

    table_ddl = get_ddl(df, table_name)

    if not table_exists:
        print(
            f"\tTable: {table_name} created in absence of pre-existing "
            f"table\n")
        continue_load = True
        snowflake.execute_query(table_ddl)

    elif table_exists and fields_match and not force_recreate:
        print(
            f"\tTable: {table_name} already exists w/ matching field names\n"
            f"- continuing load and will append data to table\n")
        continue_load = True

    elif table_exists and fields_match and force_recreate:
        print(
            f"\tTable: {table_name} Already exists w/ matching field names "
            f"- Recreated by user w/ force_recreate=True\n")
        continue_load = True
        snowflake.execute_query(table_ddl)

    elif table_exists and not fields_match and not force_recreate:
        print(
            f"\tColumns in {table_name} don't match those in local DataFrame"
            f"\n\t- Use `force_recreate=True` to overwrite existing table\n")
        continue_load = False

    elif table_exists and not fields_match and force_recreate:
        print(
            f"\tTable: {table_name} columns don't match those in local "
            f"DataFrame \n- Force-recreated by user\n")
        continue_load = True
        snowflake.execute_query(table_ddl)

    else:
        print(
            f"\tUnknown error occured w/ load of {table_name} to Snowflake "
            f"\n\t- please check snowmobile.snowloader source codes")
        continue_load = False

    return continue_load


def remove_local(file_path: str, keep_local: bool = False) -> None:
    """Removes local copy of exported file post-loading

    Args:
        file_path: path to write local file to
        keep_local: boolean value indicating whether or not to delete local
            file post-load
    Returns:
        None
    """
    if keep_local is False:
        os.remove(file_path)
        print(f"\n<Local copy of file deleted from {file_path}>")

    else:
        print(f"\n<Local copy of file saved in {file_path}>")

    return None


def df_to_snowflake(df: pd.DataFrame, table_name: str,
                    snowflake: snowquery.Snowflake = '', conn_name: str = '',
                    force_recreate: bool = False, keep_local: bool = False,
                    output_location: str = os.getcwd(),
                    on_error: str = 'continue',
                    file_format: str = 'csv_gem7318') -> object:
    """Combines the above functions to flexibly & safely load a
    DataFrame into a Snowflake table.

    # (1) Prepares DataFrame for load by standardizing column names
    #       and adding a 'LOADED_TMSTMP' field to the far right side
    # (2) Checks for existence of the table in Snowflake and compares
    #       structure of in-warehouse table to that of local DataFrame
    # (3) Defaults to creating the table if it doesn't exist, appending to the
    #       table if it exists with matching field names/data types and will
    #       forgo loading the data/return a boolean value of False if
    #       otherwise; this can be over-ridden by passing `force_recreate=True`
    #       when the function is called
    # (4) Deletes local file written out to load into a staging table
    # (5) Deletes the staging table after load is completed successfully

    Args:
        df: DataFrame to load to Snowflake
        table_name: Table name to load the data into
        force_recreate: Boolean value indicating whether or not to recreate
            the table irrelevant of matching structure between local and DB
        keep_local: Boolean value indicating whether or not to keep the
            local .csv that is written out in the process
        output_location: Location to write out local .csv to; defaults to
            current working directory
        on_error: Query parameter for how to handle loading errors
        file_format: User-defined file_format within Snowflake
    Returns:
        continue_load: Boolean value indicating whether or not load was
            successful - can be particularly useful when iterating through
            multiple files and appending to the same table and is intended
            for use similar to the following, wherein all files that match the
            structure of the table's DDL are loaded/appended and those that
            do not are returned in a dictionary containing the file's name
            and the columns of the DataFrame that failed to load;

            ```python
            problem_files = {}
            for file_name, df in dict_of_dfs.items():

                loaded = df_to_snowflake(df, name_of_table)

                if not loaded:
                    problem_files[file_name] = df.columns

            print(f"files_to_qa:\n\t{list(problem_files.keys()}")
            ```
    """
    if not snowflake:
        snowflake = snowquery.Snowflake(conn_name=conn_name)

    continue_load = verify_load(snowflake=snowflake, df=df,
                                table_name=table_name,
                                force_recreate=force_recreate)

    if continue_load:

        file_name = f"{table_name}.csv"  # File name for local copy
        file_path = os.path.join(output_location,
                                 file_name)  # Path to write to
        df.to_csv(file_path, index=False, sep='|', header=False, quotechar='"',
                  quoting=csv.QUOTE_ALL)  # Exporting csv

        create_stage = \
            f"create or replace stage {table_name}_stage file_format " \
            f"= {file_format};"

        put_path = file_path.replace('\\',
                                     '\\\\')  # Escaped path for put statement

        put_file = f"put 'file://{put_path}' @{table_name}_stage " \
                   f"auto_compress=true overwrite=true;"

        copy_into = f"copy into {table_name}\n" \
                    f"from @{table_name}_stage\n" \
                    f"on_error = '{on_error}';"

        drop_stage = f"drop stage {table_name}_stage;"

        statements = [create_stage, put_file, copy_into, drop_stage]

        # snowflake = sf.Snowflake()
        # snowflake.connect()

        for i, statement in enumerate(statements, start=1):

            try:
                result = snowflake.execute_query(statement)

                print(
                    f"\n<{i} of {len(statements)} completed>:\n\t"
                    f"{statement}\nResponse:\t")

                list_responses = \
                    [
                        f"{' '.join(col.title().split('_'))}: "
                        f"{result.iat[0, i1]}"
                        for i1, col in enumerate(list(result.columns))]

                for response in list_responses:
                    print(f"\t{response}")

            except:
                print(f"<statement failed>\n{statement}\n\n")
                break

        remove_local(file_path, keep_local)  # Defaults to delete local file

    else:
        pass

    if list(df.columns)[-1:] == ['LOADED_TMSTMP']:
        df.drop(columns=list(df.columns)[-1:], axis=1, inplace=True)
    else:
        pass

    return continue_load
