
import pandas as pd
import pyarrow.parquet as pq
from typing import Optional, List, Any, Tuple, Union, Literal, NotRequired


RESOURCE_ID_PARSE_RULES = [
        {
            # ECS Tasks
            'arn_colons': 5,
            'arn_slashes': 2,
            'condition_item_position': 5,
            'condition_item_value': 'task',
            'condition_item_operator': '==',
            'resource_type_concat_parts': [2, 5],
            'short_id_start': 6,
            'new_id_end': 7
        },
        {
            # SNS Topics
            'condition_item_position': 5,
            'condition_item_value': 'sns',
            'condition_item_operator': '==',
            'arn_colons': 5,
            'resource_type_concat_parts': [2, 5],
            'short_id_start': 6,
        },
        {
            # Catch-all
            'arn_colons': 5,
            'resource_type_concat_parts': [2, 5],
            'short_id_start': 3,
        },

        {
            'arn_colons': 6,
            'resource_type_concat_parts': [2, 5],
            'short_id_start': 6,
        },
    ]

def get_arn_parts_df_from_resource_id_series(series) -> pd.DataFrame:
    # Split the resource_id by either ":" or "/" and store the result in a new column "arn_parts"
    series_split = series.str.split(r'[:/]')

    # Count the occurrences of ":" and "/" in the original strings
    colon_count = series.str.count(':')
    slash_count = series.str.count('/')

    # Create a new DataFrame with the required structure
    df = pd.DataFrame({
        'original_resource_id': series,
        'arn_parts': series_split,
        'arn_colons': colon_count,
        'arn_slashes': slash_count
    })
    return df


def check_condition_item_keys_in_rule(d) -> None:
    keys = {'condition_item_position','condition_item_value','condition_item_operator'}
    intersect = keys.intersection(d.keys())
    all_keys_present = len(intersect) == len(keys)
    all_keys_omitted = len(intersect) == 0
    if not all_keys_present and not all_keys_omitted:
        raise Exception('Either all three condition item keys must be present in a rule or omitted from it. Partial sets not valid.')


def get_resource_type_and_short_id(row: pd.Series) -> pd.Series:
    for idx,rule in enumerate(RESOURCE_ID_PARSE_RULES):

        check_condition_item_keys_in_rule(rule)

        colon_condition = True if rule.get('arn_colons') is None else row['arn_colons'] == rule['arn_colons']
        slash_condition = True if rule.get('arn_slashes') is None else row['arn_slashes'] == rule['arn_slashes']
        #resource_type_already_has_value = False if not row.get('resource_type') else True

        if rule.get('condition_item_position'):
            comparison_value = row['arn_parts'][rule['condition_item_position']] 
            match rule['condition_item_operator']:
                case "==":
                    comparison_condition = comparison_value == rule['condition_item_value']
                case "!=":
                    comparison_condition = comparison_value != rule['condition_item_value']
                case _:
                    raise Exception(f"condition_item_operator value '{row['condition_item_operator']}' not supported")
        else:
            # if no rule provided,
            comparison_condition = True

        if (colon_condition
            and slash_condition
            and comparison_condition
           ):
            resource_type_parts = []
            # Combine one or more positional parts or hard-coded strings into the resource type
            for i in (rule.get('resource_type_concat_parts') or []):

                if isinstance(i, int):
                    part = row['arn_parts'][i]
                    resource_type_parts.append(part)
                elif isinstance(i, str):
                    resource_type_parts.append(i)
                else:
                    raise Exception(f'Unexpected type "{type(i)}"for resource_concat_positions')

            resource_type = ':'.join(resource_type_parts)
            short_resource_id = '/'.join(row['arn_parts'][rule['short_id_start']:rule.get('new_id_end')])
            return resource_type, short_resource_id
    return '',''

def parse_resource_ids_to_df(df) -> pd.DataFrame:
    # Not all resource IDs are ARNs, and we only want ARNs
    arn_mask = df['resource_id'].str[:4] == 'arn:'
    arn_df = df[arn_mask]

    # Given an ARN, split the arn into an array of string parts for every occurrence of a ":" or "/"
    arn_parts_df = get_arn_parts_df_from_resource_id_series(arn_df['resource_id'])
    arn_parts_df['resource_type'], arn_parts_df['short_resource_id'] = zip(*arn_parts_df.apply(get_resource_type_and_short_id, axis=1))

    df = pd.concat([df, arn_parts_df[['resource_type', 'short_resource_id']]], axis=1)

    return df