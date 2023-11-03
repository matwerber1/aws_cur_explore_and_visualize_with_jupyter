import glob
import json
import pandas as pd
import pyarrow.parquet as pq
from typing import Optional, List, TypedDict, Dict

from .parse_resource_ids_to_df import parse_resource_ids_to_df

class ImportMetadata(TypedDict):
    column_mapping: Dict[str, str]
    source_file_rows: int


# S3 object prefix for status table files we don't use:
FILE_PATTERNS_TO_IGNORE = [
    'cost_and_usage_data_status'
]

def ignore_file(filename: str) -> bool:
    return any(pattern_to_ignore in filename for pattern_to_ignore in FILE_PATTERNS_TO_IGNORE)

def remove_ignored_files(files: List[str]) -> List[str]:
    return [file for file in files if not ignore_file(file)]

def read_parquet_file(file_path: str, columns: List[str], row_filters: Optional[List] = None) -> pd.DataFrame:
    """Read a single Parquet file and return a DataFrame."""
    print(f'Loading {file_path}')
    parquet_table = pq.read_table(file_path, columns=columns, filters=row_filters)
    return parquet_table.to_pandas()

def read_all_parquet_files(files: List[str], columns_to_import: List[str], exclude_row_filters: Optional[List] = None) -> pd.DataFrame:
    """Read all Parquet files and concatenate them into a single DataFrame."""
    dataframes = [read_parquet_file(f, columns_to_import, exclude_row_filters) for f in files if not ignore_file(f)]

    return pd.concat(dataframes, ignore_index=True)

def rename_columns(df: pd.DataFrame, mapping_file: str) -> (pd.DataFrame, Dict[str, str]):
    """Rename DataFrame columns based on a mapping file."""
    with open(mapping_file, 'r') as json_file:
        columns_to_rename = json.load(json_file)
    df_renamed = df.rename(columns=columns_to_rename)

    # provides final mapping of old and new columns.
    actual_column_mapping = {
        original: (columns_to_rename.get(original) if original in df.columns else None)
        for original in set(df.columns)
        #for original in set(df.columns).union(columns_to_rename)   # includes mapping_file keys missing in actual file
    }
    missing_columns = [key for key, value in actual_column_mapping.items() if value is None]
    print('WARNING: "{old_col}" not in source data, unable to rename to "{columns_to_rename[old_col]}"')
    return df_renamed, actual_column_mapping


def import_cur_to_df(
    *,
    path_pattern: str,
    columns_to_import: Optional[List[str]] = None,
    exclude_row_filters: Optional[List] = None,
    new_column_names_file: Optional[str] = None,
    parse_resource_ids: Optional[bool] = True
) -> pd.DataFrame:
    """Import Parquet files from a path pattern into a DataFrame."""
    files = glob.glob(path_pattern, recursive=True)
    if not files:
        raise FileNotFoundError(f"No files found for the given pattern: {path_pattern}")

    # If no specific columns to import are provided, use the columns from the first Parquet file
    if not columns_to_import:
        metadata = pq.read_metadata(files[0])
        columns_to_import = metadata.schema.names

    df = read_all_parquet_files(files, columns_to_import, exclude_row_filters)

    if parse_resource_ids:
        df = parse_resource_ids_to_df(df)
        print("\nAdded columns 'resource_type' and 'short_resource_id' by parsing line_item_resource_id.")

    if new_column_names_file:
        df, column_mapping = rename_columns(df, new_column_names_file)
        print(f'\nColumns renamed:\n'+ '\n'.join(
            [f'  {old} -> {new}'for old, new in column_mapping.items()]
        ))

    return df

# This function call would be used in a Jupyter cell with appropriate parameters.
# df = import_cur_to_df(path_pattern="s3://bucket/path/to/parquet/*", ...)
