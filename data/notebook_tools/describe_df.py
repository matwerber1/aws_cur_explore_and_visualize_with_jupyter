import pandas as pd
from IPython.display import display

def describe_df(df: pd.DataFrame) -> None:
    """We add a row to understand what % of each column has blank or null values.
    Useful given how sparse some of the columns in AWS's report and that, in some cases,
    we have to choose from multiple similar columns.
    """
    missing_values = df.isna() | (df == '')
    non_null_percentage = ((1 - missing_values.mean()) * 100).round(1)
    desc_df = df.describe(include='all')
    desc_df.loc['non_null_pct'] = non_null_percentage

    # Display the modified description
    display(desc_df)