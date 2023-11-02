import glob
import json
from datetime import timedelta
import pandas as pd
import pyarrow.parquet as pq
from typing import Optional, List, Any, Tuple, Union, Literal, NotRequired


from .parse_resource_ids_to_df import parse_resource_ids_to_df

# Define a custom type for the operation which is a string but with limited choices
RowFilterOperationType = Union[Literal["<"], Literal["<="], Literal["=="], Literal[">"], Literal[">="], Literal["!="]]

# Define the type for the cost filter
RowFilterType = List[Tuple[str, RowFilterOperationType, float]]


def import_cur_to_df(
        *,
        path_pattern: str,
        columns_to_import: Optional[List[str]] = None,
        exclude_row_filters: Optional[RowFilterType] = None,
        new_column_names_file: Optional[str] = None,
        parse_resource_ids: Optional[bool] = True
    ) -> pd.DataFrame:
    """Import Parquet files from a path pattern.

    Args:
        path_pattern (str): path pattern to the location of your AWS Cost Explorer Parquet files.
        columns_to_import (Optional[List[str]]): list of column names you want to import (default: import all columns)
        exclude_row_filters (Optional[RowFilterType]): list of filters for rows to ignore in import (format based on filters argument for pd.read_parquet with engine=pyarrow)
        new_column_names (Optional[str]): optional JSON file containing a mapping of original column names with preferred column names

    Returns:
        pd.DataFrame: Pandas DataFrame that contains rows merged from discovered files in your provided path pattern.
    """
    files = glob.glob(path_pattern, recursive=True)
    df = pd.DataFrame()
    total_file_rows = 0
    for file in files:
        # Ignore the metadata table file(s) added by AWS to your S3 bucket
        if 'cost_and_usage_data_status' in file:
            print(f'Skipping status file {file}')
        else:
            print(f'Loading {file}')
            metadata = pq.read_metadata(file)
            actual_columns = metadata.schema.names

            if not columns_to_import:
                columns_to_import = actual_columns
            else:
                # May be mistaken, but I seem to recall sorting was important for one of the pandas operations I was doing somewhere
                columns_to_import.sort()
                missing_columns = list(set(columns_to_import).difference(actual_columns))

                if len(missing_columns) > 0:
                    raise Exception(f'Requested columns not found in Parquet file: {missing_columns}')

            excluded_columns = list(set(actual_columns).difference(columns_to_import))
            df = pd.concat([df, pd.read_parquet(file, columns=columns_to_import, engine='pyarrow', filters=exclude_row_filters)], ignore_index=True)
            print(f'==> {metadata.num_rows} rows imported from {len(columns_to_import)} columns ({len(excluded_columns)} columns ignored)\n')
            total_file_rows = total_file_rows + int(metadata.num_rows)

    min_date = df['line_item_usage_start_date'].min()
    max_date = df['line_item_usage_end_date'].max()

    number_of_days_inclusive = (max_date - min_date).days + 1
    total_cost = df['line_item_unblended_cost'].sum()
    total_rows_imported = len(df)
    excluded_row_count = total_file_rows - total_rows_imported
    print(f'\nTotal rows imported: {total_rows_imported} ({excluded_row_count} excluded due to filters)')
    print(f"  - {number_of_days_inclusive} days from {min_date} to {max_date}")
    print(f"  - Total unblended cost = {total_cost}\n")

    if new_column_names_file:
        with open(new_column_names_file, 'r') as json_file:
            columns_to_rename = json.load(json_file)
        df.rename(columns=columns_to_rename, index=columns_to_rename, inplace=True)
        print(f'Renamed columns to match {new_column_names_file}')

        # Move newly added columns so that resource_type in front of original resource ID and short_resource_id after it
        # TODO: change logic to make this step indepedent of whether or not columns are renamed
        if parse_resource_ids:
            parsed_ids_df = parse_resource_ids_to_df(df)
            df = pd.concat([df, parsed_ids_df[['resource_type', 'short_resource_id']]], axis=1)
            cols = list(df.columns)
            cols.remove('resource_type')
            cols.remove('short_resource_id')
            anchor_index = cols.index('resource_id')
            cols.insert(anchor_index, 'resource_type')
            cols.insert(anchor_index + 2, 'short_resource_id')

            # Reindex the DataFrame with the new column order
            df = df[cols]
            print("Parsed resource IDs by adding resource_type and short_resource_id columns (this parsing probably isn't perfect!)")
    return df