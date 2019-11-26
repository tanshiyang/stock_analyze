import pandas as pd


def append_column(df, column_name):
    col_name = df.columns.tolist()
    col_name.append(column_name)
    df = df.reindex(columns=col_name)
