import pandas as pd
from IPython.display import display, HTML
from pivottablejs import pivot_ui

def configure_pandas_output_size():
    print('Configuring output window settings (e.g. width, max_rows)')
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)  # None means show all rows
    pd.set_option('display.expand_frame_repr', False)  # Display columns side by side as it's supposed to be
    pd.set_option('display.width', None)  # Width of the display in characters. If set to None and `expand_frame_repr` is True, adapt the width to the terminal.
    pd.set_option('display.max_colwidth', None)
    pd.set_option('display.float_format', lambda x: '%.3f' % x)

def configure() -> None:
    """
    Configures some preferences such as display size for cell output.

    Returns:
    None
    """
    configure_pandas_output_size()