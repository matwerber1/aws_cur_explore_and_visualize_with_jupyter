
import pandas as pd
import pyarrow.parquet as pq
from typing import Optional, List, Any, Tuple, Union, Literal, NotRequired, Required, TypedDict

# Used to specify a particular pattern of ARN to look for, as well as which parts of the ARN we want to extract as a resource_type and short_resource_id
class AwsArnParsingRule(TypedDict, total=False):
    arn_colons: Required[int]
    arn_slashes: NotRequired[int]
    condition_item_position: NotRequired[int]
    condition_item_value: NotRequired[str]
    condition_item_operator: NotRequired[Literal['==', '!=', '<=', '>=', '>', '<']]
    short_id_start: Required[int]
    new_id_end: NotRequired[int]
    resource_type_concat_parts: List[Union[str, int]]


RESOURCE_ID_PARSE_RULES: List[AwsArnParsingRule] = [
    {
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
        'arn_colons': 5,
        'condition_item_position': 5,
        'condition_item_value': 'sns',
        'condition_item_operator': '==',
        'resource_type_concat_parts': [2, 5],
        'short_id_start': 6,
    },
    {
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

# Define a custom exception for missing rule keys
class IncompleteRuleException(Exception):
    pass

# Define a custom exception for unsupported operation
class UnsupportedOperatorException(Exception):
    pass

# This function checks for rule completeness and returns a boolean
def is_rule_complete(rule: AwsArnParsingRule) -> bool:
    required_keys = {'condition_item_position', 'condition_item_value', 'condition_item_operator'}
    return all(key in rule for key in required_keys)


def get_resource_type_and_short_id(arn_parts: List[str], arn_colons: int, arn_slashes: int) -> Tuple[str, str]:
    for rule in RESOURCE_ID_PARSE_RULES:
        if not is_rule_complete(rule):
            raise IncompleteRuleException('Rule is incomplete. All condition keys must be present or omitted.')

        if (
            (arn_colons == rule.get('arn_colons')) and
            (arn_slashes == rule.get('arn_slashes')) and
            (rule.get('condition_item_position') is None or
             rule['condition_item_operator'] == '==' and arn_parts[rule['condition_item_position']] == rule['condition_item_value'] or
             rule['condition_item_operator'] == '!=' and arn_parts[rule['condition_item_position']] != rule['condition_item_value'])
        ):
            resource_type_parts = [arn_parts[i] if isinstance(i, int) else i for i in rule['resource_type_concat_parts']]
            short_id_end = rule.get('new_id_end') or len(arn_parts)
            short_resource_id = '/'.join(arn_parts[rule['short_id_start']:short_id_end])
            return ':'.join(resource_type_parts), short_resource_id

    return '', ''

def matches_rule(arn_parts: List[str], rule: AwsArnParsingRule, arn_colons: int, arn_slashes: int) -> bool:
    # Check the basic ARN format matches
    if arn_colons != rule['arn_colons']:
        return False
    slash_rule = rule.get('arn_slashes')
    if slash_rule is not None:
        if arn_slashes != slash_rule:
            return False

    # Check condition item if it exists
    condition_pos = rule.get('condition_item_position')
    if condition_pos is not None:
        condition_value = arn_parts[condition_pos]
        expected_value = rule['condition_item_value']
        operator = rule['condition_item_operator']
        if operator == '==' and condition_value != expected_value:
            return False
        if operator == '!=' and condition_value == expected_value:
            return False

    return True

def apply_rule(arn_parts: List[str], rule: AwsArnParsingRule) -> Tuple[str, str]:
    arn_colons, arn_slashes = len(arn_parts) - 1, arn_parts.count('') - 1

    if matches_rule(arn_parts, rule, arn_colons, arn_slashes):
        resource_type_parts = [arn_parts[i] if isinstance(i, int) else i for i in rule['resource_type_concat_parts']]
        short_id_start = rule['short_id_start']
        short_id_end = rule.get('new_id_end', len(arn_parts))
        short_resource_id = '/'.join(arn_parts[short_id_start:short_id_end])
        return ':'.join(resource_type_parts), short_resource_id

    return '', ''

def move_column_relative_to_another(df: pd.DataFrame, reference_column: str, move_column: str, position_offset: int) -> None:
    if position_offset == 0:
        raise ValueError("Position offset cannot be zero.")

    if reference_column not in df.columns or move_column not in df.columns:
        raise ValueError(f"Either the reference column '{reference_column}' or the move column '{move_column}' does not exist in the dataframe.")

    # Find the position of the reference and move columns
    reference_position = df.columns.get_loc(reference_column)
    move_position = df.columns.get_loc(move_column)

    # Calculate the new position for the move column
    new_position = reference_position + position_offset

    # Adjust the new position if it's out of bounds
    if new_position < 0:
        new_position = 0
    elif new_position >= len(df.columns):
        new_position = len(df.columns) - 1

    # Prevent moving the column to its current position or beyond itself
    if move_position == new_position or (position_offset > 0 and new_position > move_position):
        new_position -= 1
    elif position_offset < 0 and new_position < move_position:
        new_position += 1

    # Remove the column to move and then insert it at its new position
    column_data = df.pop(move_column)
    df.insert(new_position, move_column, column_data)



def parse_resource_ids_to_df(df: pd.DataFrame) -> pd.DataFrame:
    arn_df = df[df['line_item_resource_id'].str.startswith('arn:')].copy()
    arn_df['arn_parts'] = arn_df['line_item_resource_id'].str.split(r'[:/]')

    def get_resource_details(arn_parts):
        for rule in RESOURCE_ID_PARSE_RULES:
            result = apply_rule(arn_parts, rule)
            if result != ('', ''):
                return result
        return '', ''

    results = arn_df['arn_parts'].apply(get_resource_details)
    arn_df[['resource_type', 'short_resource_id']] = pd.DataFrame(results.tolist(), index=arn_df.index)
    df = df.join(arn_df[['resource_type', 'short_resource_id']], how='left')
    move_column_relative_to_another(df, 'line_item_resource_id', 'resource_type', -1)
    move_column_relative_to_another(df, 'line_item_resource_id', 'short_resource_id', 1)
    return df

# Example usage
# df = pd.DataFrame({'resource_id': [...]})
# parsed_df = parse_resource_ids_to_df(df)