from typing import List

import pandas as pd


def dataframe_contains_columns(dataframe: pd.DataFrame, columns: List[str]) -> bool:
    return isinstance(dataframe, pd.DataFrame) and all(col in dataframe.columns for col in columns)
