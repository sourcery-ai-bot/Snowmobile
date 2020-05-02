

import snowquery as sf
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
    col; string representation of a column name
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
    """Renaming dataframe columns for Snowflake table.
    df; pd.DataFrame to be pushed to Snowflake
    """
    df['LOADED_TMSTMP'] = datetime.datetime.now()
    old_cols = list(df.columns)
    new_cols = [standardize_col(val) for val in df.columns]
    new_col_map = {k: v for k, v in zip(old_cols, new_cols)}
    df = df.rename(columns=new_col_map)
    return df


def get_ddl(df: pd.DataFrame, table_name: str) -> str:
    """Converting mysql DDL to Snowflake DDL.
    df; pd.DataFrame to push to Snowflake
    table_name; string representation of table name to load the df into
    """
    final_ddl = \
        pd.io.sql.get_schema(df, table_name).replace('CREATE TABLE',
                                                     'CREATE OR REPLACE TABLE')
    return final_ddl


def check_information_schema(table_name: str) -> list:
    """Checks information schema for existence of table/returns columns if so.
    table_name; string representation of table name to load the df into
    """

    sql = f"""SELECT
                ORDINAL_POSITION
                ,COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'
            ORDER BY 1 ASC"""

    snowflake = sf.Snowflake()
    snowflake.connect()
    validation_df = snowflake.execute_query(sql, return_results=True)
    snowflake.disconnect()

    try:
        table_cols = list(validation_df['COLUMN_NAME'])

    except:
        table_cols = []

    return table_cols


def compare_fields(df_cols: list, table_cols: list) -> int:
    """Counts number of matches between columns of df & columns of Snowflake
    table.
    Will return 0 if the table doesn't exists
    df_cols; list(df.columns)
    table_cols; list returned by check_information_schema() function
    """
    matched_list = []
    for i, (df_col, tbl_col) in enumerate(zip(df_cols, table_cols), start=1):

        if df_col == tbl_col:
            matched_list.append(i)

        else:
            pass

    matched_cnt = len(matched_list)

    return matched_cnt


def validate_table(df: pd.DataFrame, table_name: str) -> tuple:
    """Analyzes count of matching columns to count of cols in df to load.
    df; pd.DataFrame to load into Snowflake
    table_name; name of table (str) to load df into
    """
    table_cols = check_information_schema(table_name)

    df_cols = list(df.columns)

    cnt_matches = compare_fields(df_cols, table_cols)

    if cnt_matches == len(df_cols) and cnt_matches == len(table_cols):
        outcome = True, True  # Table exists & fields match

    elif len(table_cols) != 0:
        outcome = True, False  # Table exists but fields don't match

    else:
        outcome = False, False  # Table doesn't exist

    return outcome


def verify_load(df: pd.DataFrame, table_name: str,
                force_recreate: bool = False) -> bool:
    """Renames dataframe's columns, validates against table, recreates if
    needed.
    df; pd.DataFrame to push to Snowflake
    table_name; string representation of table name to load the df into
    force_recreate; boolean value to indicating whether or not to
    force-recreation of table
    """
    print(f"<validating load into {table_name}>")

    snowflake = sf.Snowflake()
    snowflake.connect()

    df = rename_cols_for_snowflake(df)

    table_exists, fields_match = validate_table(df, table_name)
    table_ddl = get_ddl(df, table_name)

    if not table_exists:
        print(
            f"\tTable: {table_name} Created in Absence of Pre-Existing "
            f"Table\n")
        continue_load = True
        snowflake.execute_query(table_ddl)

    elif table_exists and fields_match and not force_recreate:
        print(
            f"\tTable: {table_name} Already Exists w/ Matching Field Names\n")
        continue_load = True

    elif table_exists and fields_match and force_recreate:
        print(
            f"\tTable: {table_name} Already Exists w/ Matching Field Names "
            f"- Force-Recreated by User\n")
        continue_load = True
        snowflake.execute_query(table_ddl)

    elif table_exists and not fields_match and not force_recreate:
        print(
            f"\tColumns in {table_name} Don't Match Those in Local DataFrame"
            f"\n\t- Use `force_recreate=True` to Overwrite Existing Table\n")
        continue_load = False

    elif table_exists and not fields_match and force_recreate:
        print(
            f"\tTable: {table_name} Columns Don't Match Those in Local "
            f"DataFrame - Force-Recreated by User\n")
        continue_load = True
        snowflake.execute_query(table_ddl)

    else:
        print(
            f"\tUnknown Error Occured w/ Load of {table_name} to Snowflake "
            f"\n\t- Please Check SnowLoader Source Codes")
        continue_load = False

    snowflake.disconnect()

    return continue_load


def remove_local(file_path: str, keep_local: bool = False) -> None:
    """Removes local copy of exported file post-loading
    file_path; path to write local file to
    keep_local; boolean value indicating whether or not to delete file
    post-load
    """
    if keep_local is False:
        os.remove(file_path)
        print(f"\n<Local copy of file deleted from {file_path}>")

    else:
        print(f"\n<Local copy of file saved in {file_path}>")

    return None


def df_to_snowflake(df: pd.DataFrame, table_name: str,
                    force_recreate: bool = False, keep_local: bool = False,
                    output_location: str = os.getcwd(),
                    on_error: str = 'continue',
                    file_format: str = 'csv_gem7318') -> object:
    """Prepares dataframe for load, checks Snowflake for table existence,
    creates table
    if doesn't exist or invalid, appends data if not, recreates if prompted
    to do so, deletes local copy of file by default
    """
    continue_load = verify_load(df, table_name, force_recreate=force_recreate)

    if continue_load:

        file_name = f"{table_name}.csv"  # File name for local copy
        file_path = os.path.join(output_location,
                                 file_name)  # Path to write to
        df.to_csv(file_path, index=False, sep='|', header=False, quotechar='"',
                  quoting=csv.QUOTE_ALL)  # Exporting csv

        create_stage = f"create or replace stage {table_name}_stage file_format " \
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

        snowflake = sf.Snowflake()
        snowflake.connect()

        for i, statement in enumerate(statements, start=1):

            try:
                result = snowflake.execute_query(statement,
                                                 return_results=True)

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

        snowflake.disconnect()  # Disconnecting from Snowflake

    else:
        pass

    if list(df.columns)[-1:] == ['LOADED_TMSTMP']:
        df.drop(columns=list(df.columns)[-1:], axis=1, inplace=True)

    else:
        pass

    return continue_load

# TODO: Add a return value on the try/except such that a user can build
#  conditionals based on successful or unsuccessful load

# TODO: Build in functionality where user can specify that they don't want the
#   table to be recreated on column mismatch

# TODO: Fix bug where a call to df_to_snowflake() pushes dataframe to Snowflake
#   but deletes a column in the local version of the dataframe

# TODO: Run testing on if script fails or not depending on whether or not
#  the directory where your python script is set to the working directory
#  within it

# df = pd.read_excel(r'C:\Users\GEM7318\Yum! Brands, Inc\PH US BA Team - '
#                    r'Infosync Report Downloads\_2020\PACPizza LLC\YUM KPI Extract_28841_200421_033702.xlsx')
# df['fz_group'] = 'PACPizza'

# validate_table()
# validate_table(df, 'INFOSYNC_RAW_TEST2')
# check_information_schema('INFOSYNC_RAW_TEST2')

# verify_load(df, 'INFOSYNC_RAW_TEST2')

# load = df_to_snowflake(df, 'INFOSYNC_RAW_TEST2')
# if load:
#     print(1)
# else:
#     print(0)
# try:

# except:
#     print(1)

# # Example

# df = pd.read_excel(
#     r'C:\Users\GEM7318\Documents\Github\COVID-Analytics\_xlsx\GEM_Shelter_in_place_DMAs.xlsx')
# df.head()

# df2 = pd.read_csv(
#     r'\\Dalgem7318b\D\Grant Murray\TTD Ad Hocs\10-16-2019 Bain Marketing '
#     r'Acceleration\DOOH Stores Fixed.csv')
# df2.head()

# df_to_snowflake(df, 'GEM_SHELTER_IN_PLACE_DMAS', force_recreate=True)
# df_to_snowflake(df2, 'GEM_DOOH_STORE_DESC', force_recreate=True)

# pd.io.sql.get_schema(df, 'TEST_TABLE')
